# frozen_string_literal: true

require 'cassandra'
require 'zlib'

module NoSE
  module Backend
    # A backend which communicates with Cassandra via CQL
    class CassandraBackend < Backend
      include Subtype

      def initialize(model, indexes, plans, update_plans, config)
        super

        @hosts = config[:hosts]
        @port = config[:port]
        @keyspace = config[:keyspace]
        @generator = Cassandra::Uuid::Generator.new
      end

      # Generate a random UUID
      def generate_id
        @generator.uuid
      end

      # Produce the DDL necessary for column families for the given indexes
      # and optionally execute them against the server
      def indexes_ddl(execute = false, skip_existing = false,
                      drop_existing = false)
        Enumerator.new do |enum|
          @indexes.select{|index| !index.is_secondary_index}.map do |index| 
            ddl = index_cql index
            enum.yield ddl

            begin
              drop_index(index) if drop_existing && index_exists?(index)
              client.execute(ddl) if execute 
            rescue Cassandra::Errors::AlreadyExistsError => exc
              next if skip_existing

              new_exc = IndexAlreadyExists.new exc.message
              new_exc.set_backtrace exc.backtrace
              raise new_exc
            end
          end
        end
      end

      def secondary_index_ddl(has_index_hash)
        Enumerator.new do |enum|
          has_index_hash.map do |has_index|
              ddl = secondary_index_cql has_index,@indexes
              enum.yield ddl

              begin
                client.execute(ddl)
              rescue Cassandra::Errors::AlreadyExistsError => exc
                new_exc = IndexAlreadyExists.new exc.message
                new_exc.set_backtrace exc.backtrace
                raise new_exc
              end
          end
        end
      end

      # Insert a chunk of rows into an index
      # @return [Array<Array<Cassandra::Uuid>>]
      def index_insert_chunk(index, chunk)
        fields = index.all_fields.to_a
        prepared = "INSERT INTO \"#{index.key}\" (" \
                   "#{field_names fields}" \
                   ") VALUES (#{(['?'] * fields.length).join ', '})"
        prepared = client.prepare prepared

        ids = []
        client.execute(client.batch do |batch|
          chunk.each do |row|
            index_row = index_row(row, fields)
            ids << (index.hash_fields.to_a + index.order_fields).map do |field|
              index_row[fields.index field]
            end
            batch.add prepared, arguments: index_row
          end
        end)

        ids
      end

      # Check if the given index is empty
      def index_empty?(index)
        begin 
        query = "SELECT COUNT(*) FROM \"#{index.key}\" LIMIT 1"
        client.execute(query).first.values.first.zero?
        rescue => e
          p e
          return false 
        end
      end

      # Check if a given index exists in the target database
      def index_exists?(index)
        client
        @cluster.keyspace(@keyspace).has_table? index.key
      end

      # Check if a given index exists in the target database
      def drop_index(index)
        client.execute "DROP TABLE \"#{index.key}\""
      end

      # Sample a number of values from the given index
      def index_sample(index,has_index_hash, count)
        field_list = index.all_fields.map { |f| "\"#{f.id.split('.').last}\"" }
        if index.is_secondary_index
          has_index = has_index_hash.select{|has_in| has_in.index_key == index.key and has_in.index_value}[0]
          query = "SELECT #{field_list.join ', '} " \
                "FROM \"#{has_index.parent_table_id}\" LIMIT #{count}"
        else
          query = "SELECT #{field_list.join ', '} " \
                "FROM \"pdr2\".\"#{index.key}\" LIMIT #{count}" # NOTE: hard-coded
        end
        rows = client.execute(query).rows

        # XXX Ignore null values for now
        # fail if rows.any? { |row| row.values.any?(&:nil?) }

        rows
      end

      private

      # Produce an array of fields in the correct order for a CQL insert
      # @return [Array]
      def index_row(row, fields)
        fields.map do |field|
          value = row[field.id.split(".").last]
          if field.is_a?(Fields::IDField)
            value = case value
                    when Numeric
                      Cassandra::Uuid.new value.to_i
                    when String
                      value = value.gsub(Cassandra::Uuid::HYPHEN, Cassandra::Uuid::EMPTY_STRING)
                      value = value.rjust(16)[0, 16].unpack("C*").join
                      Cassandra::Uuid.new value
                    when nil
                      Cassandra::Uuid::Generator.new.uuid
                    else
                      value
                    end
          end

          value
        end
      end

      
      # Produce the CQL to create the definition for a given index
      # @return [String]
      def index_cql(index) 
        ddl = "CREATE COLUMNFAMILY \"#{index.key}\" (" \
          "#{field_names index.all_fields, true}, " \
          "PRIMARY KEY((#{field_names index.hash_fields})"


        cluster_key = index.order_fields
        ddl += ", #{field_names cluster_key}" unless cluster_key.empty?
        ddl += '))'

        ddl += " WITH caching = {'keys' : 'NONE','rows_per_partition' : 'NONE'};"

        ddl
      end

      def secondary_index_cql(has_index,indexes)
        return if has_index.nil?
        index = indexes.select{|index| index.key == has_index.index_key}.first
        return "CREATE CUSTOM INDEX #{has_index.index_key + has_index.parent_table_id} ON #{has_index.parent_table_id}(#{(field_names index.hash_fields).split(',').first}) USING 'org.apache.cassandra.index.sasi.SASIIndex';"
      end

      # Get a comma-separated list of field names with optional types
      # @return [String]
      def field_names(fields, types = false)
        fields.map do |field|
          name = "\"#{field.id.split('.').last}\""
          name += ' ' + cassandra_type(field).to_s if types
          name
        end.join ', '
      end

      # Get a Cassandra client, connecting if not done already
      def client
        return @client unless @client.nil?
        @cluster = Cassandra.cluster hosts: @hosts, port: @port,
                                     timeout: nil
        @client = @cluster.connect @keyspace
      end

      # Return the datatype to use in Cassandra for a given field
      # @return [Symbol]
      def cassandra_type(field)
        field_class = field.class
        case [field_class]
        when [Fields::BooleanField]
          :boolean
        when [Fields::IntegerField]
          :int
        when [Fields::FloatField]
          :float
        when [Fields::StringField]
          :text
        when [Fields::DateField]
          :timestamp
        when [Fields::IDField],
             [Fields::ForeignKeyField]
          :uuid
          # case field.name # FIXME: Only for observatory
          # when 'object_id'
          #   :int
          # when 'filter01'
          #   :text
          # else
          #   :uuid
          # end
        end
      end

      # Insert data into an index on the backend
      class InsertStatementStep < Backend::InsertStatementStep

        attr_reader :fields
        def initialize(client, index, fields)
          super

          @fields = fields.map(&:id) & index.all_fields.map(&:id)
          @prepared = client.prepare insert_cql
          @generator = Cassandra::Uuid::Generator.new
        end

        # Insert each row into the index
        def process(results)
          results.each do |result|
            fields = @index.all_fields.select { |field| result.key? field.id.split('.').last }

            fields.sort_by!{|field| @prepared.cql.index(field.id.split('.').last)}

            values = fields.map do |field|
              value = result[field.id.split('.').last]

              # If this is an ID, generate or construct a UUID object
              if field.is_a?(Fields::IDField)
                value = if value.nil?
                          @generator.uuid
                        else
                          Cassandra::Uuid.new(value.to_i)
                        end
              end

              # XXX Useful to test that we never insert null values
              # fail if value.nil?

              value
            end

            begin
              @client.execute(@prepared, arguments: values)
            rescue Cassandra::Errors::InvalidError
              # We hit a value which does not actually need to be
              # inserted based on the data since some foreign
              # key in the graph corresponding to this column
              # family does not exist
              nil
            end
          end
        end

        private

        # The CQL used to insert the fields into the index
        def insert_cql
          insert = "INSERT INTO #{@index.key} ("
          insert += @fields.map { |f| "\"#{f}\"" }.join(', ')
          insert << ') VALUES (' << (['?'] * @fields.length).join(', ') + ')'

          insert
        end
      end

      # Delete data from an index on the backend
      class DeleteStatementStep < Backend::DeleteStatementStep
        def initialize(client, index)
          super

          @index_keys = @index.hash_fields + @index.order_fields.to_set

          # Prepare the statement required to perform the deletion
          delete = "DELETE FROM #{index.key} WHERE "
          delete += @index_keys.map { |key| "\"#{key.id}\" = ?" }.join(' AND ')
          @prepared = client.prepare delete
        end

        # Execute the delete for a given set of keys
        def process(results)
          # Delete each row from the index
          results.each do |result|
            values = delete_values result
            @client.execute(@prepared, arguments: values)
          end
        end

        private

        # Get the values used in the WHERE clause for a CQL DELETE
        def delete_values(result)
          @index_keys.map do |key|
            cur_field = @index.all_fields.find { |field| field.id.split('.').last == key.id }

            if cur_field.is_a?(Fields::IDField)
              Cassandra::Uuid.new(result[key.id].to_i)
            else
              result[key.id]
            end
          end
        end
      end

      # A query step to look up data from a particular column family
      class IndexLookupStatementStep < Backend::IndexLookupStatementStep
        # rubocop:disable Metrics/ParameterLists
        def initialize(client, select, conditions, step, next_step, prev_step)
          super

          @logger = Logging.logger['nose::backend::cassandra::indexlookupstep']

          # TODO: Check if we can apply the next filter via ALLOW FILTERING
          cql = select_cql(select, conditions)
          # puts cql # extract query
          @prepared = client.prepare cql
        end
        # rubocop:enable Metrics/ParameterLists

        # Perform a column family lookup in Cassandra
        def process(conditions, results,query_conditions)
          results = initial_results(conditions) if results.nil?
          condition_list = result_conditions conditions, results
          new_result = fetch_all_queries condition_list, results,query_conditions

          # Limit the size of the results in case we fetched multiple keys
          new_result[0..(@step.limit.nil? ? -1 : @step.limit)]
        end

        private

        # Produce the select CQL statement for a provided set of fields
        # @return [String]
        # yusuke benchmarkの中で実際にselect句を発行している箇所
        def select_cql(select, conditions)
          select = expand_selected_fields select
          cql = "SELECT #{select.map { |f| "\"#{f.id.split('.').last}\"" }.join ', '} FROM " \
                "\"#{@step.index.key}\" WHERE #{cql_where_clause conditions}"
          cql += cql_order_by

          # Add an optional limit
          cql << " LIMIT #{@step.limit}" unless @step.limit.nil?

          cql
        end

        # Produce a CQL where clause using the given conditions
        # @return [String]
        def cql_where_clause(conditions)
          where = @eq_fields.map do |field|
            "\"#{field.id.split('.').last}\" = ?"
          end.join ' AND '
          unless @range_field.nil?
            condition = conditions.each_value.find(&:range?)
            where << " AND \"#{condition.field.id.split('.').last}\" #{condition.operator} ?"
          end

          where
        end

        # Produce the CQL ORDER BY clause for this step
        # @return [String]
        def cql_order_by
          # TODO: CQL3 requires all clustered columns before the one actually
          #       ordered on also be specified
          #
          #       Example:
          #
          #         SELECT * FROM cf WHERE id=? AND col1=? ORDER by col1, col2
          return '' if @step.order_by.empty?
          ' ORDER BY ' + @step.order_by.map { |f| "\"#{f.id}\"" }.join(', ')
        end

        # Lookup values from an index selecting the given
        # fields and filtering on the given conditions
        def fetch_all_queries(condition_list, results, query_conditions)
          new_result = []
          @logger.debug { "  #{@prepared.cql} * #{condition_list.size}" }

          # TODO: Chain enumerables of results instead
          # Limit the total number of queries as well as the query limit
          condition_list.zip(results).each do |condition_set, result|
            # Loop over all pages to fetch results
            values = lookup_values condition_set, query_conditions
            fetch_query_pages values, new_result, result

            # Don't continue with further queries
            break if !@step.limit.nil? && new_result.length >= @step.limit
          end
          @logger.debug "Total result size = #{new_result.size}"

          new_result
        end

        # Get the necessary pages of results for a given list of values
        def fetch_query_pages(values, new_result, result)
          new_results = @client.execute(@prepared, arguments: values)
          loop do
            # Add the previous results to each row
            rows = new_results.map { |row| result.merge row }

            # XXX Ignore null values in results for now
            # fail if rows.any? { |row| row.values.any?(&:nil?) }

            new_result.concat rows
            break if new_results.last_page? ||
                     (!@step.limit.nil? && result.length >= @step.limit)
            new_results = new_results.next_page
            @logger.debug "Fetched #{result.length} results"
          end
        end

        # Produce the values used for lookup on a given set of conditions
        def lookup_values(condition_set,query_conditions)
          condition_set.map do |condition|
            value = condition.value ||
              query_conditions[condition.field.id.split('.').last].value
            fail if value.nil?

            if condition.field.is_a?(Fields::IDField)
              Cassandra::Uuid.new(value.to_i)
            else
              value
            end
          end
        end
      end
    end
  end
end
