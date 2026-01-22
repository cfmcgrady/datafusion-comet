


1. 构建项目
```
make release PROFILES="-Dspotless.check.skip=true -Dscalastyle.skip=true -Drat.skip=true -Pspark-3.5" COMET_FEATURES=celeborn
```

2. 由于spark启动的shuffle manager依赖，我们需要拷贝jar包到spark的工作目录
```
cp spark/target/comet-spark-spark3.5_2.12-0.13.0-SNAPSHOT.jar ~/Software/spark-3.5.7-bin-hadoop3/jars
```

3. 如果重新构建了celeborn的Java客户端，也需要更新一下spark工作目录的jar包
```
cp ~/.m2/repository/org/apache/celeborn/celeborn-client-spark-3-shaded_2.12/0.4.2.3-SNAPSHOT/celeborn-client-spark-3-shaded_2.12-0.4.2.3-SNAPSHOT.jar ~/Software/spark-3.5.7-bin-hadoop3/jars
```
celeborn项目的目录`~/Project/incubator-celeborn`
spark celeborn Java client的打包命令
```
cd /Users/fchen/Project/incubator-celeborn && export DEFAULT_ARTIFACT_REPOSITORY=https://mvn.devops.xiaohongshu.com/repository/maven-public/ && export JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home && ./build/sbt -Pspark-3.5 celeborn-client-spark-3-shaded/publishM2
```

4. 运行测试
> 注意命令会启动一个常驻spark-shell
```
cd /Users/fchen/Software/spark-3.5.7-bin-hadoop3 && bash comet.sh
```
