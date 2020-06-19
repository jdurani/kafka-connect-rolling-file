ARG BASE_IMAGE_VERSION
ARG JAR_NAME

FROM confluentinc/cp-kafka-connect:${BASE_IMAGE_VERSION}

ADD target/${JAR_NAME} /usr/share/java/kafka-connect-rolling-file/${JAR_NAME}


ENV CONNECT_KEY_CONVERTER="org.apache.kafka.connect.converters.ByteArrayConverter" \
  CONNECT_VALUE_CONVERTER="org.apache.kafka.connect.converters.ByteArrayConverter" \
  CONNECT_INTERNAL_KEY_CONVERTER="org.apache.kafka.connect.json.JsonConverter" \
  CONNECT_INTERNAL_VALUE_CONVERTER="org.apache.kafka.connect.json.JsonConverter" \
  CONNECT_PLUGIN_PATH=/usr/share/java/ \
  CONNECT_LOG4J_ROOT_LOGLEVEL=INFO \
  KAFKA_HEAP_OPTS=-Xmx5g \
  TZ="Europe/Bratislava"
