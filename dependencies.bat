@echo off
echo -----------------------------------------
echo Installing Python Dependencies for MACN...
echo -----------------------------------------

python -m pip install --upgrade pip

pip install pyzmq
pip install networkx
pip install matplotlib

echo -----------------------------------------
echo All dependencies installed successfully!
echo You may now start working on the project.
echo -----------------------------------------

pause
