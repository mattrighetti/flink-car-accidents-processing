package it.polimi.middleware.flink.tutorial.batch.accidents.utils;

/**
 * Utility enum used to generate the mask needed to Flink
 * to decide on what should be put in a Tuple
 */
public enum AccidentField {
    DATE(28),
    TIME(27),
    BOROUGH(26),
    ZIP_CODE(25),
    LATITUDE(24),
    LONGITUDE(23),
    LOCATION(22),
    ON_STREET_NAME(21),
    CROSS_STREET_NAME(20),
    OFF_STREET_NAME(19),
    NUMBER_OF_PERSONS_INJURED(18),
    NUMBER_OF_PERSONS_KILLED(17),
    NUMBER_OF_PEDESTRIANS_INJURED(16),
    NUMBER_OF_PEDESTRIANS_KILLED(15),
    NUMBER_OF_CYCLIST_INJURED(14),
    NUMBER_OF_CYCLIST_KILLED(13),
    NUMBER_OF_MOTORIST_INJURED(12),
    NUMBER_OF_MOTORIST_KILLED(11),
    CONTRIBUTING_FACTOR_VEHICLE_1(10),
    CONTRIBUTING_FACTOR_VEHICLE_2(9),
    CONTRIBUTING_FACTOR_VEHICLE_3(8),
    CONTRIBUTING_FACTOR_VEHICLE_4(7),
    CONTRIBUTING_FACTOR_VEHICLE_5(6),
    UNIQUE_KEY(5),
    VEHICLE_TYPE_CODE_1(4),
    VEHICLE_TYPE_CODE_2(3),
    VEHICLE_TYPE_CODE_3(2),
    VEHICLE_TYPE_CODE_4(1),
    VEHICLE_TYPE_CODE_5(0);

    private final int value;

    AccidentField(int position) {
        this.value = (int) Math.pow(2, position);
    }

    public static String getFields(AccidentField... accidents) {
        int finalVal = 0;

        for (AccidentField field : accidents) {
            finalVal = finalVal | field.getValue();
        }

        return String.format("%29s", Integer.toBinaryString(finalVal)).replace(' ', '0');
    }

    public int getValue() {
        return value;
    }
}
