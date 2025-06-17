import os
import yaml
from pathlib import Path
from typing import Dict, Optional, List, Set
import re
import subprocess

class DbtProjectGenerator:
    def __init__(self, project_name: str, project_dir: str):
        """
        Initialize DBT project generator
        
        Args:
            project_name: Name of the DBT project
            project_dir: Directory where project will be created
        """
        # Clean project name to be DBT compatible by replacing spaces with underscores
        self.project_name = project_name.replace(' ', '_')
        self.project_dir = Path(project_dir)
        
    def create_project_structure(self, layers: Set[str]):
        """
        Creates the basic DBT project structure using dbt init
        
        Args:
            layers: Set of layer names to create directories for
        """
        # Create project directory if it doesn't exist
        os.makedirs(self.project_dir, exist_ok=True)
        
        # Run dbt init
        subprocess.run(['dbt', 'init', self.project_name], cwd=self.project_dir.parent)
        
        # Create model directories for each layer
        for layer in layers:
            os.makedirs(self.project_dir / 'models' / layer, exist_ok=True)
            
        # Create profiles.yml in the correct location
        dbt_dir = Path('/opt/app-root/src/.dbt')
        dbt_dir.mkdir(parents=True, exist_ok=True)
            
        # Create profiles.yml in .dbt directory
        profiles_config = {
            self.project_name: {
                'target': 'dev',
                'outputs': {
                    'dev': {
                        'type': 'trino',
                        'host': 'localhost',
                        'port': 8080,
                        'user': 'admin',
                        'password': 'admin',
                        'catalog': 'hive',
                        'schema': 'default',
                        'threads': 4
                    }
                }
            }
        }
        
        profiles_file = dbt_dir / 'profiles.yml'
        with open(profiles_file, 'w') as f:
            yaml.dump(profiles_config, f, default_flow_style=False, sort_keys=False)
            
    def create_model_from_sql(
        self,
        sql_content: str,
        model_name: str,
        layer: str,
        config: Optional[Dict] = None
    ) -> None:
        """
        Creates a model file using dbt codegen
        
        Args:
            sql_content: SQL query content
            model_name: Name of the model
            layer: Model layer (staging/intermediate/mart)
            config: Optional model configurations
        """
        # Clean model name
        model_name = re.sub(r'[^a-zA-Z0-9_]', '_', model_name)
        
        # Create model directory if it doesn't exist
        model_dir = self.project_dir / 'models' / layer
        os.makedirs(model_dir, exist_ok=True)
        
        # Generate model using dbt codegen
        model_path = model_dir / f'{model_name}.sql'
        
        # Write SQL content to temporary file
        temp_sql_path = self.project_dir / 'temp.sql'
        with open(temp_sql_path, 'w') as f:
            f.write(sql_content)
        
        # Generate model using dbt codegen
        subprocess.run([
            'dbt', 'codegen', 
            '--project-dir', str(self.project_dir),
            '--profiles-dir', '/opt/app-root/src/.dbt',
            '--target-path', str(self.project_dir / 'target'),
            '--output', str(model_path),
            '--sql', str(temp_sql_path)
        ])
        
        # Clean up temporary file
        os.remove(temp_sql_path)
        
        # Add config block if provided
        if config:
            with open(model_path, 'r') as f:
                content = f.read()
            
            # Format config parameters
            config_params = []
            for key, value in config.items():
                if isinstance(value, str):
                    config_params.append(f"{key}='{value}'")
                elif isinstance(value, list):
                    config_params.append(f"{key}={value}")
                else:
                    config_params.append(f"{key}={value}")
            
            config_str = ',\n    '.join(config_params)
            config_block = f"{{{{ config(\n    {config_str}\n) }}}}\n\n"
            
            # Write content with config block
            with open(model_path, 'w') as f:
                f.write(config_block + content)

def generate_dbt_project(
    project_name: str,
    project_dir: str,
    models_config: Dict,
    sql_base_dir: str
) -> None:
    """
    Generates a DBT project from configuration and SQL files
    
    Args:
        project_name: Name of the DBT project
        project_dir: Directory where project will be created
        models_config: Dictionary containing model configurations
        sql_base_dir: Base directory containing SQL files
    """
    # Initialize project generator
    generator = DbtProjectGenerator(project_name, project_dir)
    
    # Extract unique layers from configuration
    layers = {model['layer'] for model in models_config['models']}
    
    # Create project structure with dynamic layers
    generator.create_project_structure(layers)
    
    # Create models
    for model in models_config['models']:
        model_name = model['name']
        sql_file = model.get('sql')
        
        if not sql_file:
            raise ValueError(f"SQL file path not specified for model: {model_name}")
            
        # Read SQL content from file
        sql_path = Path(sql_base_dir) / sql_file
        if not sql_path.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_path}")
            
        with open(sql_path, 'r') as f:
            sql_content = f.read()
            
        generator.create_model_from_sql(
            sql_content=sql_content,
            model_name=model_name,
            layer=model['layer'],
            config=model.get('config')
        )

def load_models_config(config_file: str) -> Dict:
    """
    Load models configuration from YAML file
    
    Args:
        config_file: Path to YAML configuration file
        
    Returns:
        Dictionary containing model configurations
    """
    with open(config_file, 'r') as f:
        return yaml.safe_load(f)

# Example usage
if __name__ == "__main__":
    # Load configuration from YAML file
    models_config = load_models_config('models.yml')
    
    # Generate DBT project
    generate_dbt_project(
        project_name="my_analytics",
        project_dir="./my_dbt_project",
        models_config=models_config,
        sql_base_dir="."
    )