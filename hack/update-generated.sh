#!/bin/bash

function cleanup()
{
   rm -rf build
}
trap cleanup EXIT

export GOROOT=$(dirname $(dirname $(which go)));
mkdir build && echo "Appease operator-sdk" >> build/Dockerfile && operator-sdk generate k8s && cleanup
