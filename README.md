For each key value pair:
-- key is less than 127 bytes;
-- value is less than Integer.Max bytes;

Shall support:
1. GET key -- return the value;
2. UPDATE key, value -- update the key with the value, is key is not existed, then create it;
3. DELETE key -- delete the key
4. CAS key, old value, new value -- Compare and set the key

The operation shall be atomic.

To build docker image, you can run:

```bash
mvn package dockerfile:build
```

You shall add the following configuration segment to the maven's settings.xml:

```xml
  <pluginGroups>
    <!-- pluginGroup
     | Specifies a further group identifier to use for plugin lookup.
    <pluginGroup>com.your.plugins</pluginGroup>
    -->
    <pluginGroup>com.spotify</pluginGroup>
  </pluginGroups>
```
