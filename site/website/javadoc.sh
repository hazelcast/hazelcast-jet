#!/bin/bash

find ../javadoc -name "*.tar.gz" -exec tar xzvf {} -Cstatic/javadoc \;
