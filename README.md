# master_thesis-app
First set up the swarm:
    
    powershell -executionpolicy bypass -File C:\Users\drago\IdeaProjects\master_thesisB\infrastructure\docker-machine-VMs\swarm-node-hibrid-setup.ps1 

Wait till the service is up:
    
    $managerZero="vmNode1"
    $StackName="TheStackOfDani"
    docker-machine ssh $managerZero "docker stack services $StackName"

Then log into the Spark Master container:

    docker-machine ssh $managerZero "docker ps -a"
    docker-machine ssh $managerZero "docker exec -it ContainerId /bin/bash"

And Submit those apps
SparkPI example:

    spark-submit --class org.apache.spark.examples.SparkPi \
        --deploy-mode client \
        --master spark://spark-master:7077 \
        examples/jars/spark-examples_2.11-2.2.1.jar 1000

Or submit your own:
 
    Create the .jar: http://spark.apache.org/docs/latest/quick-start.html#self-contained-applications
    and place it on ($SPARK_HOME)/app
    place a README.md there too

Then submit the Spark app to the Spark master container:
    
    spark-submit --class "SimpleApp" \
        --deploy-mode client \
        --master spark://spark-master:7077 \
        app/master_thesis-app_2.11-0.1.jar

When you are done clean the swarm:

    powershell -executionpolicy bypass -File C:\Users\drago\IdeaProjects\master_thesisB\infrastructure\docker-machine-VMs\swarm-node-hibrid-teardown.ps1