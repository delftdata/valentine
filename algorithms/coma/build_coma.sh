#!/bin/bash

# THIS SCRIPT REQUIRES SUBVERSION AND MAVEN INSTALLED
set -x

# DOWNLOAD COMA SOURCES
svn checkout http://svn.code.sf.net/p/coma-ce/mysvn/coma-project && \
cd coma-project && \
find . -type f -exec dos2unix {} \; && \
cd .. && \

# PATCH COMA TO BE COMPATIBLE WITH THE VALENTINE BENCHMARK
patch -ruN -p1 -d  coma-project < valentine.patch && \

# BUILD THE PATCHED COMA
cd coma-project && \
mvn clean && \
mvn -Dmaven.test.skip=true package && \

# MOVE THE CREATED JAR TO THE CORRECT FOLDER
cd .. && \
mkdir artifact && \
mkdir coma_output && \
mv coma-project/coma-engine/target/coma-engine-0.1-CE-SNAPSHOT-jar-with-dependencies.jar artifact/coma.jar && \

# CLEANUP
rm -rf coma-project
