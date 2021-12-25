# Spark structured streaming arbitrary stateful operations with deltalake: <br> Case breakpoint transformation

In breakpoint time series only changed values are indicated and therefore the data must be to transformed to equally sampled data before using in most time series algorithms. To see example system input and output press [here](https://kaskeaja.github.io/spark-stateful-streaming/breakpoint_to_equally_sampled.html) 

Repository content:
* Breakpoint writer, spark structured streaming and Jupyter lab are run separate docker containers
* Breakpoint writer outputs to delta lake
* Spark structured streaming reads from deltalake and writes back to another table in deltalake
* Folders
    * shared-with-docker: **Accessible from host and all three docker containers**
    * shared-with-docker/jupyter: **Code to run Jupyter lab notebook**
    * shared-with-docker/writer: **Breakpoint writer**
    * shared-with-docker/processor: **Breakpoint transformation**
    * shared-with-docker/delta-lake-storage: **Deltalake**

## Docker compose

Building images
```
docker compose build
```

Spark writer container shell
```
docker compose run --service-ports spark-writer
```

Spark processor container shell
```
docker compose run --service-ports spark-processor
```

Jupyter lab container shell
```
docker compose run --service-ports jupyter bash
```

Remove all containers
```
docker compose down
```

## Jupyter lab ###

In container run
```
jupyter lab --ip='*'
```

Note that ```--ip='*'``` makes jupyter lab to allow access outside localhost.


## Scalastyle
To check coding style using [Scalastyle](http://www.scalastyle.org) write in sbt projects
```
sbt scalastyle
```

## ScalaTest
To run tests using [ScalaTest](https://www.scalatest.org/) write in sbt projects
```
sbt test
```
