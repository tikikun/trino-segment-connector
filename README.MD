## Trino to Segment

![img.png](img.png "Trino Segment")
Sending data directly from Trino to Segment

### Some words

I am tikikun, currently learning more about coding in Java. I built this thing to use as a tool in my work as well as to learn Java.

If you have the time to read and want to have contribution please open PR. I want to have other people review my code so that i can be a better Java programmer.

### Dependencies

* Requirements specified in the pom of the project

### Installing

* Before running maven build, please make sure you have inputted the writekey into the analytics object

The analytics writekey will be sitting inside TrinoSegmentClient Object here is how it looks like

```java
    static final Analytics analytics = Analytics.builder("Input your key here").log(stdout).build();
```

Command to build the project

``
mvn clean install -DskipTests
``

### Usage example
```sql
select sendTrack('Userid here','event name you want',
cast(
row(column_a,column_b)
as row(keynameforcolumna_a varchar, keynameforcolumn_b varchar)
) from sometable
```

## Authors

Tikikun

## Acknowledgments
I will update the code I user from multiple libraries here after having the time
