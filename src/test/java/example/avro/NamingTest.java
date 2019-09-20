package example.avro;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class NamingTest {

    @Test
    public void differentNamesInAvroAndJava() {
        var naming = Naming.newBuilder()
                .setFavoriteColor("blue")
                .setFavoriteNumber(42)
                .setName("Hans Dampf")
                .build();


        //the field that is specified in the avro schema as "favorite_number" can be accessed in Java using standard CamelCase conventions. No need to use java conventions in the avro schema
        assertThat(naming.getFavoriteNumber()).isEqualTo(naming.get("favorite_number"));
    }


}
