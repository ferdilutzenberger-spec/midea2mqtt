import logging
import json
import yaml
import time
import sys
import signal
from typing import Optional, Dict, Any
from datetime import datetime
import paho.mqtt.client as mqtt
from midea_beautiful import appliance_state

__VERSION__ = "Midea2MQTT v0.3.0 (hardened)"
_CONFIG_FILE = "/etc/opt/midea2mqtt/midea2mqtt.yml"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
_LOGGER = logging.getLogger(__name__)

class JsonEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle non-serializable objects"""
    def default(self, obj):
        try:
            if hasattr(obj, '__dict__'):
                return str(obj)
            return super().default(obj)
        except TypeError:
            return str(obj)

class Midea2Mqtt():
    """Main application class for Midea2MQTT bridge"""

    def __init__(self):
        self.online = False
        self.refreshDelay = 60
        self.mqtt_client: Optional[mqtt.Client] = None
        self.appliances: Dict[str, 'MideaAppliance'] = {}
        self.mqtt_reconnect_count = 0
        self.mqtt_last_reconnect: Optional[datetime] = None
        self.running = True

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

        _LOGGER.info(__VERSION__)
        
        try:
            with open(_CONFIG_FILE) as file:
                try:
                    data = yaml.safe_load(file)
                    if not data:
                        _LOGGER.error(f"Config file {_CONFIG_FILE} is empty")
                        sys.exit(1)
                except yaml.YAMLError as exception:
                    _LOGGER.error(f"unable to parse yaml from {_CONFIG_FILE}")
                    _LOGGER.exception(exception)
                    sys.exit(1)
        except FileNotFoundError:
            _LOGGER.error(f"Config file {_CONFIG_FILE} not found")
            sys.exit(1)
        except IOError as e:
            _LOGGER.error(f"unable to read config file {_CONFIG_FILE}")
            _LOGGER.exception(e)
            sys.exit(1)

        try:
            valid = self._parseConfigGeneral(data.get("general", {}))
            valid = self._parseConfigMqtt(data.get("mqtt", {})) if valid else False
            valid = self._parseConfigAppliances(data.get("devices", [])) if valid else False
            
            if not valid:
                _LOGGER.error("Invalid configuration")
                sys.exit(1)
            
            if not self.appliances:
                _LOGGER.error("No appliances configured")
                sys.exit(1)
            
            valid = self._connectMqtt() if valid else False
            if not valid:
                _LOGGER.error("Failed to connect to MQTT broker")
                sys.exit(1)
            
            valid = self._connectAppliances() if valid else False
            if not valid:
                _LOGGER.warning("No appliances connected (will retry)")

            _LOGGER.info(f"init complete: poll and publish every {self.refreshDelay} seconds")
            self.mqtt_client.loop_start()
            self._main_loop()
        except Exception as e:
            _LOGGER.error("Fatal error during initialization")
            _LOGGER.exception(e)
            sys.exit(1)
        finally:
            if self.mqtt_client:
                self.mqtt_client.loop_stop()
            _LOGGER.info("shutdown complete")

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        _LOGGER.info(f"Received signal {signum}, shutting down...")
        self.running = False

    def _main_loop(self):
        """Main polling loop"""
        while self.running:
            try:
                time.sleep(self.refreshDelay)
                failed_appliances = []
                
                for topic, appliance in self.appliances.items():
                    try:
                        _LOGGER.debug(f"Refreshing {topic}")
                        payload = appliance.refresh()
                        if payload:
                            self.mqtt_client.publish(topic, payload)
                    except Exception as e:
                        _LOGGER.error(f"Error refreshing appliance {topic}")
                        _LOGGER.exception(e)
                        failed_appliances.append((topic, appliance, e))
                
                # Retry failed appliances periodically
                for topic, appliance, error in failed_appliances:
                    if not appliance.connected:
                        try:
                            appliance.connect()
                            _LOGGER.info(f"Reconnected appliance {topic}")
                        except Exception as e:
                            _LOGGER.debug(f"Reconnection attempt for {topic} failed: {e}")
                        
            except Exception as e:
                _LOGGER.error("Error in main loop")
                _LOGGER.exception(e)
                time.sleep(5)  # Wait before retrying

    def _parseConfigGeneral(self, config: Dict[str, Any]) -> bool:
        """Parse general configuration section"""
        try:
            if not isinstance(config, dict):
                _LOGGER.warning("General config is not a dict, using defaults")
                return True
            
            pollrate = config.get("pollrate", 60)
            if not isinstance(pollrate, (int, float)) or pollrate <= 0:
                _LOGGER.warning(f"Invalid pollrate {pollrate}, using default 60")
                pollrate = 60
            self.refreshDelay = pollrate
            
            loglevel = config.get("loglevel", "INFO")
            if loglevel and loglevel.upper() in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']:
                logging.getLogger().setLevel(getattr(logging, loglevel.upper()))
            
            return True
        except Exception as e:
            _LOGGER.error("Error parsing general config")
            _LOGGER.exception(e)
            return False

    def _parseConfigMqtt(self, config: Dict[str, Any]) -> bool:
        """Parse MQTT configuration section"""
        try:
            if not isinstance(config, dict):
                _LOGGER.error("MQTT config is not a dict")
                return False
            
            self.mqttBroker = config.get("broker", "").strip()
            if not self.mqttBroker:
                _LOGGER.error("MQTT broker not configured")
                return False
            
            port = config.get("port", 1883)
            if not isinstance(port, int) or port <= 0 or port > 65535:
                _LOGGER.warning(f"Invalid MQTT port {port}, using 1883")
                port = 1883
            self.mqttPort = port
            
            self.mqttUsername = config.get("username", "").strip()
            self.mqttPassword = config.get("password", "").strip()
            self.mqttClientid = config.get("clientid", "midea2mqtt").strip()
            self.mqttBasetopic = config.get("basetopic", "midea").strip()
            
            return True
        except Exception as e:
            _LOGGER.error("Error parsing MQTT config")
            _LOGGER.exception(e)
            return False

    def _parseConfigAppliances(self, config: list) -> bool:
        """Parse devices configuration section"""
        try:
            if not isinstance(config, list):
                _LOGGER.error("Devices config is not a list")
                return False
            
            if len(config) == 0:
                _LOGGER.error("No devices configured")
                return False
            
            self.appliances = {}
            applianceCount = 0
            
            for idx, config_entry in enumerate(config):
                try:
                    if not isinstance(config_entry, dict):
                        _LOGGER.error(f"Device config entry {idx} is not a dict")
                        continue
                    
                    # Validate required fields
                    required_fields = ["topic", "address", "token", "key"]
                    missing = [f for f in required_fields if f not in config_entry]
                    if missing:
                        _LOGGER.error(f"Device {idx} missing fields: {missing}")
                        continue
                    
                    topic = f"{self.mqttBasetopic}/{config_entry['topic']}"
                    newAppliance = MideaAppliance(
                        topic, config_entry["address"],
                        config_entry["token"], config_entry["key"]
                    )
                    
                    if newAppliance.valid:
                        applianceCount += 1
                        self.appliances[topic] = newAppliance
                    else:
                        _LOGGER.warning(f"Device {idx} is not valid")
                        
                except Exception as e:
                    _LOGGER.error(f"Error parsing device config entry {idx}")
                    _LOGGER.exception(e)
                    continue
            
            return applianceCount > 0
        except Exception as e:
            _LOGGER.error("Error parsing devices config")
            _LOGGER.exception(e)
            return False

    def _connectAppliances(self) -> bool:
        """Connect to all configured appliances"""
        applianceOnlineCount = 0
        
        for topic, appliance in self.appliances.items():
            try:
                if appliance.connect():
                    applianceOnlineCount += 1
                    try:
                        payload = appliance.refresh()
                        if payload:
                            self.mqtt_client.publish(topic, payload)
                    except Exception as e:
                        _LOGGER.warning(f"Error publishing initial state for {topic}")
                        _LOGGER.exception(e)
                else:
                    _LOGGER.warning(f"Failed to connect to appliance {topic}")
            except Exception as e:
                _LOGGER.error(f"Error connecting to appliance {topic}")
                _LOGGER.exception(e)
        
        return applianceOnlineCount > 0

    def _connectMqtt(self) -> bool:
        """Connect to MQTT broker with retry logic"""
        try:
            self.mqtt_client = mqtt.Client(
                client_id=self.mqttClientid, userdata=None, 
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2
            )

            # Set callbacks
            self.mqtt_client.on_connect = self._on_connect
            self.mqtt_client.on_message = self._on_message
            self.mqtt_client.on_disconnect = self._on_disconnect

            # Set username and password if given
            if self.mqttUsername:
                try:
                    self.mqtt_client.username_pw_set(self.mqttUsername, self.mqttPassword)
                except Exception as e:
                    _LOGGER.warning("Failed to set MQTT credentials")
                    _LOGGER.exception(e)

            try:
                _LOGGER.info(f"Connecting to MQTT broker {self.mqttBroker}:{self.mqttPort}")
                self.online = self.mqtt_client.connect(self.mqttBroker, self.mqttPort) == 0
                return self.online
            except Exception as e:
                _LOGGER.error(f"Failed to connect to MQTT broker")
                _LOGGER.exception(e)
                return False
        except Exception as e:
            _LOGGER.error("Error setting up MQTT client")
            _LOGGER.exception(e)
            return False

    def _on_message(self, client, userdata, msg):
        """Handle incoming MQTT messages"""
        try:
            _LOGGER.debug(f"{msg.topic}: {msg.payload}")
            
            # Remove trailing "/set" from topic
            if not msg.topic.endswith("/set"):
                _LOGGER.debug(f"Ignoring message from non-setter topic: {msg.topic}")
                return
            
            topic = msg.topic[:-4]
            if topic not in self.appliances:
                _LOGGER.warning(f"no midea appliance named {topic}")
                return
            
            appliance = self.appliances[topic]
            if appliance.parseSetMsg(msg.payload):
                try:
                    payload = appliance.refresh()
                    if payload:
                        self.mqtt_client.publish(topic, payload)
                except Exception as e:
                    _LOGGER.error(f"Error publishing state for {topic}")
                    _LOGGER.exception(e)
        except Exception as e:
            _LOGGER.error("Error processing MQTT message")
            _LOGGER.exception(e)

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        """Handle MQTT connection"""
        try:
            if reason_code.is_failure:
                _LOGGER.error(f"Failed to connect to '{self.mqttBroker}': {reason_code}")
                self.mqtt_reconnect_count += 1
            else:
                self.mqtt_reconnect_count = 0
                self.mqtt_last_reconnect = datetime.now()
                _LOGGER.info(f"Connected to MQTT broker '{self.mqttBroker}':{self.mqttPort}")
                
                # (re)subscribe to topics
                self._subscribeToTopic((self.mqttBasetopic, "set"))
                for topic in self.appliances:
                    self._subscribeToTopic((topic, "set"))
        except Exception as e:
            _LOGGER.error("Error in MQTT connect callback")
            _LOGGER.exception(e)
    
    def _on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties):
        """Handle MQTT disconnection"""
        if reason_code == 0:
            _LOGGER.info("Disconnected from MQTT broker")
        else:
            _LOGGER.warning(f"Unexpected MQTT disconnection: {reason_code}")

    def _subscribeToTopic(self, topic: Any) -> bool:
        """Subscribe to MQTT topic"""
        try:
            topic_str = "/".join(topic) if isinstance(topic, tuple) else topic
            result = self.mqtt_client.subscribe(topic_str)
            
            if result[0] == 0:
                _LOGGER.debug(f"Subscribed to: {topic_str}")
                return True
            else:
                _LOGGER.warning(f"Failed to subscribe to: {topic_str} (code: {result[0]})")
                return False
        except Exception as e:
            _LOGGER.error(f"Error subscribing to topic")
            _LOGGER.exception(e)
            return False



class MideaAppliance():
    """Wrapper for Midea appliance with error handling"""

    def __init__(self, topic: str, address: str, token: str, key: str):
        self.valid = self._validate_params(topic, address, token, key)
        self.topic = topic
        self.address = address
        self.token = token
        self.key = key
        self._appliance = None
        self.connected = False
        self.last_error: Optional[str] = None

    def _validate_params(self, topic: str, address: str, token: str, key: str) -> bool:
        """Validate appliance parameters"""
        if not all([topic, address, token, key]):
            _LOGGER.error("Invalid appliance parameters (empty values)")
            return False
        return True

    def connect(self) -> bool:
        """Connect to the appliance"""
        if not self.valid:
            _LOGGER.error(f"Cannot connect to {self.topic}: invalid parameters")
            return False
        
        try:
            _LOGGER.debug(f"Connecting to appliance {self.topic} at {self.address}")
            self._appliance = appliance_state(
                address=self.address, token=self.token, key=self.key,
            )
            self.connected = True
            self.last_error = None
            _LOGGER.info(f"Connected to device {self.topic} (type: {self._appliance.type})")
            return True
        except Exception as e:
            self.connected = False
            self.last_error = str(e)
            _LOGGER.error(f"Failed to connect to appliance {self.topic}")
            _LOGGER.exception(e)
            return False

    def refresh(self) -> Optional[str]:
        """Refresh appliance state and return JSON payload"""
        if not self.connected or not self._appliance:
            raise RuntimeError(f"Appliance {self.topic} is not connected")
        
        try:
            self._appliance.refresh()
            
            # prepare state as json (=> publish via mqtt)
            data = {}
            for attr in dir(self._appliance.state):
                if not attr.startswith('_') and not callable(getattr(self._appliance.state, attr)):
                    try:
                        data[attr] = getattr(self._appliance.state, attr)
                    except Exception as e:
                        _LOGGER.debug(f"Error getting attribute {attr}: {e}")
                        continue
            
            payload = json.dumps(data, cls=JsonEncoder)
            _LOGGER.debug(f"Refreshed {self.topic}")
            return payload
        except Exception as e:
            self.last_error = str(e)
            _LOGGER.error(f"Error refreshing appliance {self.topic}")
            _LOGGER.exception(e)
            raise

    def parseSetMsg(self, payload: bytes) -> bool:
        """Parse and apply setter command from MQTT message"""
        if not self.connected or not self._appliance:
            _LOGGER.warning(f"Appliance {self.topic} is not connected, ignoring command")
            return False
        
        try:
            if isinstance(payload, bytes):
                payload = payload.decode('utf-8')
            
            _LOGGER.debug(f"parseSetMsg for {self.topic}: {payload}")
            data = json.loads(payload)
            
            if not isinstance(data, dict):
                _LOGGER.error(f"Invalid JSON object for {self.topic}")
                return False
            
            for key, value in data.items():
                try:
                    setattr(self._appliance.state, key, value)
                    _LOGGER.debug(f"Set {self.topic}.{key} = {value}")
                except Exception as e:
                    _LOGGER.warning(f"Failed to set {self.topic}.{key} = {value}")
                    _LOGGER.exception(e)
            
            if hasattr(self._appliance.state, 'needs_refresh') and self._appliance.state.needs_refresh:
                try:
                    self._appliance.apply()
                    _LOGGER.debug(f"Applied changes to {self.topic}")
                except Exception as e:
                    _LOGGER.error(f"Error applying changes to {self.topic}")
                    _LOGGER.exception(e)
                    return False
            
            return True
        except json.JSONDecodeError as e:
            _LOGGER.error(f"parseSetMsg: invalid JSON for {self.topic}")
            _LOGGER.exception(e)
            return False
        except Exception as e:
            _LOGGER.error(f"Error parsing set message for {self.topic}")
            _LOGGER.exception(e)
            return False

# Start app
if __name__ == "__main__":
    mideaMqtt = Midea2Mqtt()
