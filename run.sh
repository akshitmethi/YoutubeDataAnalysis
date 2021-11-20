
#Run this file on suffice intervals
echo "start..."
echo "please be patient"
source pyVenv/bin/activate
cd src
python analysis.py
cd ..
deactivate
echo "Please find the data and excel file present in data directory"
