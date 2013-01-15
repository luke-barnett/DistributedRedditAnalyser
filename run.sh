if [ ! -d "storm-0.8.1" ]; then
    echo "Downloading storm"
    curl https://dl.dropbox.com/u/133901206/storm-0.8.1.zip > /tmp/storm.zip
    echo "Download done"
    if zipinfo /tmp/storm.zip > /dev/null; then
        echo "Unziping storm"
        unzip /tmp/storm.zip > /dev/null
        echo "Unzip done"
        rm /tmp/storm.zip
    else
        rm /tmp/storm.zip
        exit
    fi
fi

if [ -d "results" ]; then
    mv "results" "results("$(date +%F-%T)")"
fi
mkdir results

rm -rf target
echo "Compiling"
nohup mvn package > results/maven.log 2>&1
cp target/*-jar-with-dependencies.jar results/uberjar.jar
rm -rf target
echo "Compiled. Trying to run"

storm-0.8.1/bin/storm jar results/uberjar.jar distributedRedditAnalyser.Main $@
