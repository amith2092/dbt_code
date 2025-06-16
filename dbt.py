import os
import yaml
from pathlib import Path
from typing import Dict, Optional, List
import re

class DbtProjectGenerator:
    def __init__(self, project_name: str, project_dir: str):
        """
        Initialize DBT project generator
        
        Args:
            project_name: Name of the DBT project
            project_dir: Directory where project will be created
        """
        self.project_name = project_name
        self.project_dir = Path(project_dir)
        
    def create_project_structure(self):
        """Creates the basic DBT project structure"""
        # Create main project directories
        dirs = [
            'models/staging',
            'models/intermediate',
            'models/mart',
            'macros',
            'tests',
            'seeds',
            'analyses',
            'snapshots'
        ]
        
        for dir_path in dirs:
            os.makedirs(self.project_dir / dir_path, exist_ok=True)
            
        # Create dbt_project.yml
        project_config = {
            'name': self.project_name,
            'version': '1.0.0',
            'config-version': 2,
            'profile': self.project_name,
            
            'model-paths': ['models'],
            'seed-paths': ['seeds'],
            'test-paths': ['tests'],
            'analysis-paths': ['analyses'],
            'macro-paths': ['macros'],
            'snapshot-paths': ['snapshots'],
            
            'models': {
                self.project_name: {
                    'staging': {
                        'materialized': 'view',
                        'schema': 'staging'
                    },
                    'intermediate': {
                        'materialized': 'table',
                        'schema': 'intermediate'
                    },
                    'mart': {
                        'materialized': 'table',
                        'schema': 'mart'
                    }
                }
            }
        }
        
        with open(self.project_dir / 'dbt_project.yml', 'w') as f:
            yaml.dump(project_config, f, default_flow_style=False, sort_keys=False)
            
    def create_model_from_sql(
        self,
        sql_content: str,
        model_name: str,
        layer: str,
        config: Optional[Dict] = None
    ) -> None:
        """
        Creates a model file from SQL content
        
        Args:
            sql_content: SQL query content
            model_name: Name of the model
            layer: Model layer (staging/intermediate/mart)
            config: Optional model configurations
        """
        # Validate layer
        valid_layers = ['staging', 'intermediate', 'mart']
        if layer not in valid_layers:
            raise ValueError(f"Layer must be one of {valid_layers}")
            
        # Clean model name
        model_name = re.sub(r'[^a-zA-Z0-9_]', '_', model_name)
        
        # Prepare model content
        model_content = []
        
        # Add config block if provided
        if config:
            # Ensure schema is set based on layer if not specified
            if 'schema' not in config:
                config['schema'] = layer
                
            config_yaml = yaml.dump({'config': config}, default_flow_style=False)
            model_content.extend([
                '{{ config(',
                f'{config_yaml}'.replace('config:', '').strip(),
                ') }}'
            ])
            
        # Add SQL content
        model_content.append(sql_content.strip())
        
        # Write model file
        model_path = self.project_dir / 'models' / layer / f'{model_name}.sql'
        with open(model_path, 'w') as f:
            f.write('\n\n'.join(model_content))

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
    
    # Create project structure
    generator.create_project_structure()
    
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
    models_config = load_models_config('test.yml')
    
    # Generate DBT project
    generate_dbt_project(
        project_name="my_analytics",
        project_dir="./my_dbt_project",
        models_config=models_config,
        sql_base_dir="."  # Base directory containing SQL files
    )