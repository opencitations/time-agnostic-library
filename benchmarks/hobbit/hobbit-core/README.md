[![Java CI with Maven](https://github.com/hobbit-project/core/actions/workflows/maven.yml/badge.svg)](https://github.com/hobbit-project/core/actions/workflows/maven.yml)[![Codacy Badge](https://app.codacy.com/project/badge/Grade/cb81b2831ce84a4287ff588e83881b0a)](https://app.codacy.com/gh/hobbit-project/core/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_grade)[![Codacy Badge](https://app.codacy.com/project/badge/Coverage/cb81b2831ce84a4287ff588e83881b0a)](https://app.codacy.com/gh/hobbit-project/core/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_coverage)

# core
This library offers some main functionalities and utilities for the Hobbit platform project. For more information take a look at the project web page (<http://www.project-hobbit.eu>) or at the Hobbit platform project on github (<https://github.com/hobbit-project/platform>).

## Building

`test` target needs RabbitMQ running. You may need to set RabbitMQ host in `src/test/java/org/hobbit/core/TestConstants.java`.

Docker container with RabbitMQ can be started using `./start_test_environment.sh` or the one from the HOBBIT platform can be used as well.

## Using it with Maven

```xml
  <repositories>
    <repository>
      <id>maven.aksw.internal</id>
      <name>University Leipzig, AKSW Maven2 Repository</name>
      <url>http://maven.aksw.org/repository/internal</url>
    </repository>
    <repository>
      <id>maven.aksw.snapshots</id>
      <name>University Leipzig, AKSW Maven2 Repository</name>
      <url>http://maven.aksw.org/repository/snapshots</url>
    </repository>
    ...
  </repositories>

  <dependencies>
    <!-- Hobbit core -->
    <dependency>
      <groupId>org.hobbit</groupId>
      <artifactId>core</artifactId>
      <version>1.0.22</version>
    </dependency>
    ...
  </dependencies>
```
