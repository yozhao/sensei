kafka-0.7.0.jar was built against scala 2.9.2 using following steps:

git clone http://git-wip-us.apache.org/repos/asf/kafka.git
cd kafka
git checkout 0.7.0
vim ./project/build.properties #change scala version to 2.9.2
./sbt update
./sbt package
find -name *kafka*.jar
