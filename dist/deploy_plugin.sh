#/bin/bash

function usage {
    echo "usage: [$0 --host HOSTNAME] [--docker-container DOCKER_CONTAINER] SOURCE_FILE TARGET" >&2
    echo ", where one of --host or --docker-container must be specified." >&2
    exit 1
}

if [ $# -ne 4 ]
then
    usage
fi

while [ $# -gt 2 ]
do
    case $1 in
    "--host")
        host=$2
        killbillHost=$host
        shift
        shift
        ;;
    "--docker-container")
        dockerContainer=$2
        killbillHost=localhost
        shift
        shift
        ;;
    *)
        usage
        ;;
    esac
done

sourceFile=$1
target=$2
targetDir=$(dirname $target)

set -x

# exit on error
set -e

if [ -n "$dockerContainer" ]
then
    docker exec $dockerContainer bash -c "rm -rf $(dirname $targetDir)/* && mkdir -p $targetDir"
    docker cp $sourceFile $dockerContainer:$target
fi

if [ -n "$host" ]
then
    ssh $host "sudo rm -rf $(dirname $targetDir)/* && sudo mkdir -p $targetDir && sudo chmod a+w $targetDir"
    scp $sourceFile $host:$target
fi

curl -k -X POST \
https://$killbillHost:7000/1.0/kb/nodesInfo \
-H 'Authorization: Basic YWRtaW46ZnJlZWwwMGs=' \
-H 'Content-Type: application/json' \
-d '{
         "nodeCommandProperties": [
             {
                 "key": "pluginName",
                 "value": "killbill-adyen"
             }
         ],
         "nodeCommandType": "RESTART_PLUGIN",
         "isSystemCommandType": true
}'
