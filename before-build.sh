# universal-install
apt=`command -v apt-get`
yum=`command -v yum`
apk=`command -v apk`

if [ -n "$apt" ]; then
    apt-get update
    apt-get -y install gdal-devel
elif [ -n "$yum" ]; then
    yum -y install gdal-devel
elif [ -n "$apk" ]; then
    apk add gdal-dev
else
    echo "Err: no path to apt-get/yum/apk" >&2;
    exit 1;
fi