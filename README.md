# Spark structured streaming arbitrary stateful operations with deltalake: <br> Case breakpoint transformation

In breakpoint time series only changed values are indicated and therefore the data must be to transformed to equally sampled data before using in most time series algorithms. To see example system input and output press [here](https://kaskeaja.github.io/spark-stateful-streaming/breakpoint_to_equally_sampled.html) 

System description:
* Breakpoint writer, spark structured streaming and Jupyter lab are run separate docker containers
* Breakpoint writer outputs to [Delta Lake](https://delta.io/)
* Spark structured streaming reads from [Delta Lake](https://delta.io/) and writes back to another table in [Delta Lake](https://delta.io/)
* Breakpoint transformation is done using [Spark structured streaming arbitrary statefull operations](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#arbitrary-stateful-operations) and more specifically by [flatMapGroupsWithState](https://jaceklaskowski.gitbooks.io/spark-structured-streaming/content/spark-sql-streaming-KeyValueGroupedDataset-flatMapGroupsWithState.html) in append output mode.

Repository content:
* Repository root: Docker files
* shared-with-docker: **Accessible from host and all three docker containers**
* shared-with-docker/jupyter: **Code to run Jupyter lab notebook**
* shared-with-docker/writer: **Breakpoint writer**
* shared-with-docker/processor: **Breakpoint transformation**
* shared-with-docker/delta-lake-storage: [Delta Lake](https://delta.io/)

## Usage

### Docker
Building images (add --no-cache to ignore cache if needed)
```
docker compose build
```

Remove all containers
```
docker compose down
```

Remove all images
```
docker compose down --rmi all
```

### Writer
Start **writer** container in interactive mode
```
docker compose run --service-ports spark-writer
```

Start **writer** by running these commands inside the container
```
cd shared_with_host/writer
./package_and_run.sh
```

### Processor
Start **processor** container in interactive mode
```
docker compose run --service-ports spark-processor
```

Start **processor** by running these commands inside the container. Note that **processor** assumes that writer process output table exists in Delta Lake so start writer before **processor**.
```
cd shared_with_host/processor
./package_and_run.sh
```

### Jupyter lab to inspect results
Start **Jupyter** container in interactive mode
```
docker compose run --service-ports jupyter bash
```

In container run
```
cd shared_with_host/jupyter
./pyspark_notebook.sh
```

Note:
* *pyspark_notebook.sh* starts Jupyter lab with PySpark and prints out a link to open Jupyter lab in host system browser.
* There is a notebook *read_deltalake.ipynb* that you can use to inspect the both breakpoint and processed data.
* [Bokeh](https://bokeh.org/) plot is written to file *breakpoint_to_equally_sampled.html* in Jupyter folder. You can open the Bokeh plot by using a browser or Jupyter lab.


## Tests and linting

To check coding style using [Scalastyle](http://www.scalastyle.org) write in sbt projects
```
sbt scalastyle
```

To run tests using [ScalaTest](https://www.scalatest.org/) write in sbt projects
```
sbt test
```
