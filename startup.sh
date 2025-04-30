# 1) Check and install required system packages with opkg (only if missing).
REQUIRED_PACKAGES="python3-light python3-pip git git-http"

for pkg in $REQUIRED_PACKAGES; do
  if ! opkg list-installed | grep -q "^$pkg "; then
    echo "updating opkg..."
    opkg update
    echo "Installing $pkg..."
    opkg install "$pkg"
  else
    echo "$pkg is already installed. Skipping..."
  fi
done

# 2) Fast Python package installation using requirements.txt approach
# Define required packages and their versions
PIP_PACKAGES="\
paho-mqtt==2.1.0 \
pymodbus==3.8.3 \
pyserial==3.5 \
setuptools==58.1.0 \
"

# Create a temporary requirements file
echo "$PIP_PACKAGES" > /tmp/requirements.txt

# Get currently installed packages in a single operation
python3 -m pip freeze > /tmp/installed.txt

# Compare and install only missing or wrong version packages
python3 - << 'EOF'
import sys

# Read requirements
with open('/tmp/requirements.txt') as f:
    required = dict(line.strip().split('==') for line in f if '==' in line)

# Read installed packages
installed = {}
with open('/tmp/installed.txt') as f:
    for line in f:
        if '==' in line:
            name, version = line.strip().split('==')
            installed[name.lower()] = version

# Determine what needs to be installed
to_install = []
for name, version in required.items():
    if name.lower() not in installed or installed[name.lower()] != version:
        to_install.append(f"{name}=={version}")

# Print packages to install for the shell script
print('\n'.join(to_install))
EOF
) > /tmp/to_install.txt

# Install only what's needed (if anything)
if [ -s /tmp/to_install.txt ]; then
    echo "Installing missing/outdated packages..."
    python3 -m pip install --no-cache-dir -r /tmp/to_install.txt
else
    echo "All Python packages are already at the correct versions."
fi

# Clean up temporary files
rm -f /tmp/requirements.txt /tmp/installed.txt /tmp/to_install.txt

echo "All done."

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