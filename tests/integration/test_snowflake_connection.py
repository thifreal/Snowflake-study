"""
Integration tests for Snowflake ETL pipeline
Requires Snowflake credentials in config/credentials.yaml
"""
import pytest
import sys
from pathlib import Path
from unittest.mock import Mock, patch

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from config.connections import SnowflakeConnector, ConfigLoader


class TestSnowflakeConnection:
    """Integration tests for Snowflake connection"""
    
    @pytest.fixture
    def config(self):
        """Fixture to provide test configuration"""
        return {
            'user': 'test_user',
            'password': 'test_password',
            'account': 'test_account',
            'warehouse': 'compute_wh',
            'database': 'test_db',
            'schema': 'test_schema',
            'role': 'sysadmin'
        }
    
    def test_connector_initialization(self, config):
        """Test connector can be initialized"""
        connector = SnowflakeConnector(config)
        assert connector.config == config
        assert connector.connection is None
    
    @patch('config.connections.connect')
    def test_connection_success(self, mock_connect, config):
        """Test successful connection"""
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        connector = SnowflakeConnector(config)
        result = connector.connect()
        
        assert result is mock_connection
        mock_connect.assert_called_once()
    
    @patch('config.connections.connect')
    def test_connection_failure(self, mock_connect, config):
        """Test connection failure handling"""
        mock_connect.side_effect = Exception("Connection failed")
        
        connector = SnowflakeConnector(config)
        
        with pytest.raises(Exception):
            connector.connect()
    
    def test_context_manager(self, config):
        """Test connector works as context manager"""
        with patch('config.connections.connect') as mock_connect:
            mock_connection = Mock()
            mock_connect.return_value = mock_connection
            
            with SnowflakeConnector(config) as conn:
                assert conn == mock_connection


class TestConfigLoader:
    """Tests for configuration loading"""
    
    @patch('builtins.open', create=True)
    @patch('yaml.safe_load')
    def test_load_config_success(self, mock_yaml, mock_open):
        """Test successful config loading"""
        test_config = {'key': 'value'}
        mock_yaml.return_value = test_config
        
        config = ConfigLoader.load_config('config.yaml')
        
        assert config == test_config
    
    @patch('builtins.open', side_effect=FileNotFoundError())
    def test_load_config_file_not_found(self, mock_open):
        """Test config loading with missing file"""
        with pytest.raises(FileNotFoundError):
            ConfigLoader.load_config('nonexistent.yaml')
    
    @patch('builtins.open', create=True)
    @patch('yaml.safe_load')
    def test_load_credentials_success(self, mock_yaml, mock_open):
        """Test successful credentials loading"""
        test_creds = {
            'snowflake': {
                'user': 'test_user',
                'password': 'test_pass',
                'account': 'test_account'
            }
        }
        mock_yaml.return_value = test_creds
        
        creds = ConfigLoader.load_credentials('credentials.yaml')
        
        assert creds == test_creds['snowflake']
    
    @patch('builtins.open', create=True)
    @patch('yaml.safe_load')
    def test_load_credentials_missing_field(self, mock_yaml, mock_open):
        """Test credentials loading with missing required field"""
        test_creds = {
            'snowflake': {
                'user': 'test_user',
                'password': 'test_pass'
                # Missing 'account'
            }
        }
        mock_yaml.return_value = test_creds
        
        with pytest.raises(ValueError):
            ConfigLoader.load_credentials('credentials.yaml')


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
