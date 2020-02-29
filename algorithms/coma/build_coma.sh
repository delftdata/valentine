#!/bin/bash

# THIS SCRIPT REQUIRES SUBVERSION AND MAVEN INSTALLED
set -x

# DOWNLOAD COMA SOURCES
svn checkout http://svn.code.sf.net/p/coma-ce/mysvn && \

# PATCH COMA TO BE COMPATIBLE WITH THE VALENTINE BENCHMARK
cp -a mysvn/coma-project . && \
patch -ruN -p1 -d coma-project < valentine.patch && \

# BUILD THE PATCHED COMA
cd coma-project && \
mvn -Dmaven.test.skip=true clean package && \

# MOVE THE CREATED JAR TO THE CORRECT FOLDER
cd .. && \
mv coma-project/coma-engine/target/coma-engine-0.1-CE-SNAPSHOT-jar-with-dependencies.jar artifact/coma.jar && \

# CLEANUP
rm -rf mysvn && \
rm -rf coma-project
