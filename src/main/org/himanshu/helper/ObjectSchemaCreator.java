package org.himanshu.helper;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;

/**
 * Created by himanshu on 8/20/2017.
 * Class used to create object schema using input json file
 */
public class ObjectSchemaCreator {


    private static ObjectSchema objectSchema;


    public static ObjectSchema getObjectSchema(String jsonFilePath) throws IOException {
        if (objectSchema == null) {

            ObjectMapper mapper = new ObjectMapper();
            try {
                objectSchema = mapper.readValue(new File(jsonFilePath), ObjectSchema.class);

                System.out.println(objectSchema.getSchema().size());
                return objectSchema;
            } catch (IOException e) {
                throw e;
            }
        } else {
            return objectSchema;
        }
    }


}
