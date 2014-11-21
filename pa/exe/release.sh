time_stamp=$(date +%Y-%m-%d-%T)
mkdir -p "/root/release/pa/${time_stamp}"
ln -s  "/root/release/${time_stamp}" /release/pa/current/
#copy folder ddf-conf
cp -r ../ddf-conf/* .
