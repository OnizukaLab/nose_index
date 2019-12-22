FROM cassandra:3.11

#cassandra-cliは実行後に「cassandra-cli」と入力するだけで使える。実行時にホスト上のスクリプトのスキーマを流し込むくらいの事はしても良いかも。

RUN sed -i -e 's/batch_size_fail_threshold_in_kb: 50/batch_size_fail_threshold_in_kb: 100000/' /etc/cassandra/cassandra.yaml \
 && sed -i -E 's/_timeout_in_ms: [0-9]+/_timeout_in_ms: 6000000/g' /etc/cassandra/cassandra.yaml
#RUN echo "batch_size_fail_threshold_in_kb: 9192" >> /etc/cassandra/cassandra.yaml
WORKDIR /tools
COPY ./create_keyspace.sh .
