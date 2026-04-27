import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from mission_control.biorobot_controller_directional_mqtt import main

if __name__ == "__main__":
    main()