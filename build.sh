#!/bin/bash

cd ./base
docker image build -t demo/hadoopha . --no-cache
cd ..

cd ./workers
docker image build -t demo/workers . --no-cache
cd ..

cd ./journal_1
docker image build -t demo/journalnode1 . --no-cache
cd ..

cd ./journal_2
docker image build -t demo/journalnode2 . --no-cache
cd ..

cd ./journal_3
docker image build -t demo/journalnode3 . --no-cache
cd ..

echo "All images built successfully!"
