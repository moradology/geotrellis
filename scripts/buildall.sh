#!/bin/bash

./sbt -J-Xmx2G "project proj4" test || { exit 1; }
./sbt -J-Xmx2G "project vector-test" test || { exit 1; }
./sbt -J-Xmx2G "project raster-test" test || { exit 1; }
./sbt -J-Xmx2G "project spark" test  || { exit 1; }
./sbt -J-Xmx2G "project s3" test  || { exit 1; }
./sbt -J-Xmx2G "project accumulo" test  || { exit 1; }
./sbt -J-Xmx2G "project spark-etl" compile  || { exit 1; }
./sbt -J-Xmx2G "project engine-test" test || { exit 1; }
./sbt -J-Xmx2G "project slick" test:compile || { exit 1; }
./sbt -J-Xmx2G "project shapefile" compile || { exit 1; }
./sbt -J-Xmx2G "project util" compile || { exit 1; }
./sbt -J-Xmx2G "project raster-testkit" compile || { exit 1; }
./sbt -J-Xmx2G "project vector-testkit" compile || { exit 1; }
./sbt -J-Xmx2G "project spark-testkit" compile || { exit 1; }
