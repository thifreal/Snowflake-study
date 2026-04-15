"""
Snowflake connection utilities and configuration management
"""
import logging
from typing import Optional, Dict, Any
import yaml
from snowflake.connector import connect
from snowflake.connector.connection import SnowflakeConnection

logger = logging.getLogger(__name__)


class SnowflakeConnector:
    """Manages Snowflake database connections"""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Snowflake connector
        
        Args:
            config: Configuration dictionary with connection parameters
        """
        self.config = config
        self.connection: Optional[SnowflakeConnection] = None

    def connect(self) -> SnowflakeConnection:
        """
        Establish connection to Snowflake
        
        Returns:
            SnowflakeConnection object
        """
        try:
            self.connection = connect(
                user=self.config['user'],
                password=self.config['password'],
                account=self.config['account'],
                warehouse=self.config.get('warehouse'),
                database=self.config.get('database'),
                schema=self.config.get('schema'),
                role=self.config.get('role'),
            )
            logger.info(f"Connected to Snowflake account: {self.config['account']}")
            return self.connection
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {str(e)}")
            raise

    def disconnect(self) -> None:
        """Close Snowflake connection"""
        if self.connection:
            self.connection.close()
            logger.info("Disconnected from Snowflake")

    def execute_query(self, query: str) -> Any:
        """
        Execute a single query
        
        Args:
            query: SQL query to execute
            
        Returns:
            Query results
        """
        if not self.connection:
            raise Exception("Not connected to Snowflake. Call connect() first.")
        
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            return cursor.fetchall()
        finally:
            cursor.close()

    def execute_script(self, script_path: str) -> None:
        """
        Execute SQL script from file
        
        Args:
            script_path: Path to SQL script file
        """
        with open(script_path, 'r') as f:
            script = f.read()
        
        queries = script.split(';')
        cursor = self.connection.cursor()
        
        try:
            for query in queries:
                query = query.strip()
                if query:
                    cursor.execute(query)
                    logger.info(f"Executed query: {query[:50]}...")
        finally:
            cursor.close()

    def __enter__(self):
        """Context manager entry"""
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()


class ConfigLoader:
    """Loads configuration from YAML files"""

    @staticmethod
    def load_config(config_path: str) -> Dict[str, Any]:
        """
        Load configuration from YAML file
        
        Args:
            config_path: Path to configuration file
            
        Returns:
            Configuration dictionary
        """
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            logger.info(f"Loaded configuration from {config_path}")
            return config
        except Exception as e:
            logger.error(f"Failed to load configuration: {str(e)}")
            raise

    @staticmethod
    def load_credentials(credentials_path: str) -> Dict[str, Any]:
        """
        Load Snowflake credentials from YAML file
        
        Args:
            credentials_path: Path to credentials file
            
        Returns:
            Credentials dictionary
        """
        config = ConfigLoader.load_config(credentials_path)
        required_fields = ['user', 'password', 'account']
        
        for field in required_fields:
            if field not in config.get('snowflake', {}):
                raise ValueError(f"Missing required field: {field}")
        
        return config['snowflake']
