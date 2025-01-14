#!/bin/sh

# Define log file
LOG_FILE="run_modbusMQTT.log"

# Redirect stdout and stderr to the log file
exec > >(tee -a "$LOG_FILE") 2>&1

# Function to check internet connection
wait_for_internet() {
  echo "Checking for internet connection..."
  while ! ping -c 1 -q google.com &>/dev/null; do
    echo "No internet connection. Retrying in 5 seconds..."
    sleep 5
  done
  echo "Internet connection established."
}

# Wait for internet connection
wait_for_internet

# Update and install required packages using opkg
echo "Updating opkg and installing necessary packages..."
opkg update
opkg install python3-light
opkg install python3-pip
opkg install git git-http

# Install required Python packages
echo "Installing Python dependencies..."
pip install paho-mqtt pymodbus pyserial

# Navigate to the parent directory (modify this if needed)
#cd ./root || { echo "Failed to navigate to the root directory."; exit 1; }

# Remove the existing folder if it exists
if [ -d "modbusMQTT" ]; then
  echo "Removing existing modbusMQTT folder..."
  rm -rf modbusMQTT
fi

# Clone the repository
echo "Cloning the repository..."
git clone https://github.com/Tobias3sprong/modbusMQTT.git

# Run the Python script
echo "Running the Python script..."
python modbusMQTT/main.py
