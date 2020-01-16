#!/bin/bash

JET_EE_LOCAL_PATH=$1
HAZELCAST_SNAPSHOT_VERSION=4.0-SNAPSHOT
USER_GIT_REMOTE_REPO_NAME=origin
MVN_REPOSITORY=~/.m2/repository
ORIGINAL_PATH=`pwd`

# get latest version of Hazelcast snapshot
echo "Starting get latest version of Hazelcast snapshot section"
mvn dependency:get -DgroupId=com.hazelcast -DartifactId=hazelcast -Dversion=${HAZELCAST_SNAPSHOT_VERSION} -Dpackaging=jar -Dmaven.repo.local=${MVN_REPOSITORY}
mvn dependency:get -DgroupId=com.hazelcast -DartifactId=hazelcast-spring -Dversion=${HAZELCAST_SNAPSHOT_VERSION} -Dpackaging=jar -Dmaven.repo.local=${MVN_REPOSITORY}

# extract latest version
echo "Starting extract latest version section"
HAZELCAST_LATEST_VERSION=$(xmlstarlet sel -t -v "(//value/text())[1]" ${MVN_REPOSITORY}/com/hazelcast/hazelcast/${HAZELCAST_SNAPSHOT_VERSION}/maven-metadata-snapshot-repository.xml)
HAZELCAST_SPRING_LATEST_VERSION=$(xmlstarlet sel -t -v "(//value/text())[1]" ${MVN_REPOSITORY}/com/hazelcast/hazelcast-spring/${HAZELCAST_SNAPSHOT_VERSION}/maven-metadata-snapshot-repository.xml)
echo "Latest Hazelcast version is: ${HAZELCAST_LATEST_VERSION}"
echo "Latest Hazelcast Spring version is: ${HAZELCAST_SPRING_LATEST_VERSION}"
if [ -z "$HAZELCAST_LATEST_VERSION" ]; then
    echo "Latest Hazelcast version was not correctly extracted."
    exit 1
fi
if [ -z "$HAZELCAST_SPRING_LATEST_VERSION" ]; then
    echo "Latest Hazelcast version was not correctly extracted."
    exit 1
fi

# prepare branch
echo "Starting prepare branch section"
BRANCH_NAME="update-to-version-${HAZELCAST_LATEST_VERSION}"
git checkout -b ${BRANCH_NAME}

# change version
echo "Starting change version section"
sed -i "s#<hazelcast.version>.*</hazelcast.version>#<hazelcast.version>${HAZELCAST_LATEST_VERSION}</hazelcast.version>#" pom.xml
sed -i "s#<hazelcast.spring.version>.*</hazelcast.spring.version>#<hazelcast.spring.version>${HAZELCAST_SPRING_LATEST_VERSION}</hazelcast.spring.version>#" pom.xml

# try to compile
echo "Starting try to compile section"
mvn clean install -DskipTests -Dmaven.repo.local=${MVN_REPOSITORY}
if [ $? -ne 0 ]; then
    echo "Hazelcast Jet compilation did not finish successfully.";
    exit 1
fi

# get latest version of Hazelcast EE snapshot
cd ${JET_EE_LOCAL_PATH}
echo "Starting get latest version of Hazelcast snapshot section"
mvn dependency:get -DgroupId=com.hazelcast -DartifactId=hazelcast-enterprise -Dversion=${HAZELCAST_SNAPSHOT_VERSION} -Dpackaging=jar -Dmaven.repo.local=${MVN_REPOSITORY}

# extract latest version
echo "Starting extract latest EE version section"
HAZELCAST_EE_LATEST_VERSION=$(xmlstarlet sel -t -v "(//value/text())[1]" ${MVN_REPOSITORY}/com/hazelcast/hazelcast-enterprise/${HAZELCAST_SNAPSHOT_VERSION}/maven-metadata-private-snapshot-repository.xml)
echo "Latest Hazelcast EE version is: ${HAZELCAST_EE_LATEST_VERSION}"
if [ -z "$HAZELCAST_EE_LATEST_VERSION" ]; then
    echo "Latest Hazelcast EE version was not correctly extracted."
    exit 1
fi

# prepare branch
echo "Starting prepare branch section"
BRANCH_NAME="update-to-version-${HAZELCAST_LATEST_VERSION}"
git checkout -b ${BRANCH_NAME}

# change version
echo "Starting change version section"
sed -i "s#<hazelcast.version>.*</hazelcast.version>#<hazelcast.version>${HAZELCAST_LATEST_VERSION}</hazelcast.version>#" pom.xml
sed -i "s#<hazelcast.ee.version>.*</hazelcast.ee.version>#<hazelcast.ee.version>${HAZELCAST_EE_LATEST_VERSION}</hazelcast.ee.version>#" pom.xml

# try to compile
echo "Starting try to compile section"
mvn clean install -DskipTests -Dmaven.repo.local=${MVN_REPOSITORY}
if [ $? -ne 0 ]; then
    echo "Hazelcast Jet EE compilation did not finish successfully.";
    exit 1
fi

# push branch
cd ${ORIGINAL_PATH}
echo "Starting push branch section"
git add pom.xml
git commit -m "Updated to version ${HAZELCAST_LATEST_VERSION}"
git push ${USER_GIT_REMOTE_REPO_NAME} ${BRANCH_NAME}

# clean up
echo "Starting clean up section"
git checkout master
git branch -D ${BRANCH_NAME}

# push EE branch
cd ${JET_EE_LOCAL_PATH}
echo "Starting push EE branch section"
git add pom.xml
git commit -m "Updated to version ${HAZELCAST_LATEST_VERSION}"
git push ${USER_GIT_REMOTE_REPO_NAME} ${BRANCH_NAME}

# clean up EE
echo "Starting clean up EE section"
git checkout master
git branch -D ${BRANCH_NAME}
