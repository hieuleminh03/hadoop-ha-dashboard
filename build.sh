#!/bin/bash

cd ./BaseHadoop
docker image build -t demo/hadoopha . --no-cache
cd ..

cd ./Workers
docker image build -t demo/workers . --no-cache
cd ..

cd ./JournalNode1
docker image build -t demo/journalnode1 . --no-cache
cd ..

cd ./JournalNode2
docker image build -t demo/journalnode2 . --no-cache
cd ..

cd ./JournalNode3
docker image build -t demo/journalnode3 . --no-cache
cd ..

cd ./dashboard
docker image build -t demo/hadoop-dashboard . --no-cache
cd ..

echo "All images built successfully!"
echo "Dashboard will be available at: http://localhost:3000"
