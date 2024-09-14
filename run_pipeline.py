import yaml
from src.pipeline import run_pipeline

def load_config():
    with open('config/config.yaml', 'r') as file:
        return yaml.safe_load(file)


if __name__ == "__main__":
    config = load_config()
    run_pipeline(config)
