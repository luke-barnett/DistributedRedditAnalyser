if [ -d "RELEASE" ]; then
    mv "RELEASE" "RELEASE("$(date +%F-%T)")"
fi
mkdir RELEASE

rm -rf target
nohup mvn package > RELEASE/maven.log 2>&1
cp target/*-jar-with-dependencies.jar RELEASE/uberjar.jar
rm -rf target

echo "storm<0.9.0-wip7> jar RELEASE/uberjar.jar <MainClass> <arg1, args2,...>"
