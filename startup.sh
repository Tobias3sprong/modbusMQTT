
# 1) Check and install required system packages with opkg (only if missing).
REQUIRED_PACKAGES="python3-light python3-pip git git-http"

for pkg in $REQUIRED_PACKAGES; do
  if ! opkg list-installed | grep -q "^$pkg "; then
    echo "Installing $pkg..."
    opkg install "$pkg"
  else
    echo "$pkg is already installed. Skipping..."
  fi
done

# 2) Check and install specific Python packages (with exact versions).
#    Because ash doesn't support Bash arrays or split easily,
#    we'll store them in a space-separated variable and parse one by one.

PIP_PACKAGES="\
paho-mqtt==2.1.0 \
pymodbus==3.8.3 \
pyserial==3.5 \
setuptools==58.1.0 \
"

for pkg in $PIP_PACKAGES; do
  # pkg will look like "paho-mqtt==2.1.0"
  # We split on "==". One approach is to replace "==" with a space, then split that.
  namever="$(echo "$pkg" | sed 's/==/ /')"
  pkg_name="$(echo "$namever" | awk '{print $1}')"
  pkg_version="$(echo "$namever" | awk '{print $2}')"

  # Check installed version of the package
  installed_version="$(python3 -m pip show "$pkg_name" 2>/dev/null | grep '^Version:' | awk '{print $2}')"

  if [ "$installed_version" != "$pkg_version" ]; then
    echo "Installing $pkg_name==$pkg_version..."
    python3 -m pip install --no-cache-dir "$pkg_name==$pkg_version"
  else
    echo "$pkg_name $pkg_version is already installed. Skipping..."
  fi
done

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