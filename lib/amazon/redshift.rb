require "aws-sdk"
require "sequel"

module Amazon
  module Redshift
    def self.connection(redshift_url)
      Sequel.connect(
        redshift_url,
        :client_min_messages => false,
        :force_standard_strings => false,
        :disable_insert_returning => true
      )
    end

    class Table
      def initialize(schema, table_name, connection)
        @schema = schema
        @table_name = table_name
        @connection = connection
        @schema_sym = schema.to_sym
        @table_name_sym = table_name.to_sym
      end

      def reload_from_bucket(bucket, iam_role, encoding: "UTF8", ignore_header: true)
        count_before = count

        @connection.transaction do
          @connection[Sequel.qualify(@schema_sym, @table_name_sym)].truncate

          @connection.run(_s3_copy_statement(bucket, iam_role, encoding, ignore_header))

          raise Sequel::Rollback if count < count_before
        end
      end

      def reload_from_sql(sql_string)
        count_before = count

        @connection.transaction do
          @connection[Sequel.qualify(@schema_sym, @table_name_sym)].delete

          @connection.run(_sql_insert_into_statement(sql_string))

          raise Sequel::Rollback if count < count_before
        end
      end

      def rebuild_from_sql(sql_string)
        count_before = count

        @connection.transaction do
          @connection.run(_sql_drop_table_statement)
          @connection.run(_sql_create_table_as_statement(sql_string))

          raise Sequel::Rollback if count < count_before
        end
      end

      def unload_to_bucket(bucket, iam_role)
        output_file = _generate_s3_unload_filename(bucket.url)
        query = _s3_unload_statement(iam_role, output_file)
        @connection.run(query)
        output_file
      end

      def fetch_meta_data
        ds = @connection[_meta_data_query, "#{@schema}.#{@table_name}", @table_name, @schema]
        ds.call(:select)
      end

      def fetch_column_names
        _get_columns
      end

      def method_missing(m, *args, &block)
        qualified_table = Sequel.qualify(@schema_sym, @table_name_sym)
        if @connection[qualified_table].respond_to?(m)
          @connection[qualified_table].send(m, *args, &block)
        else
          super
        end
      end

      def respond_to_missing?(method_name, include_private = false)
        @connection[@table_name].respond_to?(method_name, include_private)
      end

      private

      def _s3_copy_statement(bucket, iam_role, encoding, ignore_header)
        "COPY #{@table_name_sym}
        FROM '#{bucket.url}'
        iam_role '#{iam_role}'
        region '#{bucket.region}'
        FORMAT CSV ENCODING #{encoding}#{ignore_header ? ' IGNOREHEADER 1' : ''}"
      end

      def _generate_s3_unload_filename(bucket_url)
        "#{bucket_url}/#{@schema}/#{@table_name}/#{Time.now.strftime("%Y-%m-%d")}_#{@table_name}"
      end

      def _s3_unload_statement(iam_role, output_file)
        select_query = _generate_query_string_to_include_headers

        "UNLOAD ('#{select_query}')
        TO '#{output_file}'
        IAM_ROLE '#{iam_role}'
        DELIMITER AS ','
        NULL AS ''
        ALLOWOVERWRITE
        ADDQUOTES
        PARALLEL OFF;"
      end

      def _get_columns
        query = <<QUERY
          SELECT column_name FROM information_schema.columns
          WHERE table_name = '#{@table_name}'
          AND table_schema = '#{@schema}'
          ORDER BY ordinal_position;
QUERY
        @connection[query].all.map { |c| c[:column_name] }
      end

      def _generate_query_string_to_include_headers
        columns = _get_columns
        column_castings = columns.map { |c| "REPLACE(CAST(#{c} AS text), \\'\"\\', \\'\"\"\\') AS #{c}" }.join(", ")
        "SELECT #{column_castings} FROM #{@schema}.#{@table_name}"
      end

      def _meta_data_query
        <<QUERY
          SELECT
            cols.column_name,
            cols.data_type,
            pg_catalog.col_description(c.oid, cols.ordinal_position::int) as comments,
            pg_catalog.obj_description(?::regclass, 'pg_class') as table_comments
          FROM
            information_schema.columns cols
          JOIN
            pg_catalog.pg_class c
          ON
            cols.table_name = c.relname
          WHERE
            cols.table_name = ? AND
            cols.table_schema = ?
          ORDER BY
            cols.ordinal_position::int;
QUERY
      end

      def _sql_insert_into_statement(sql_string)
        "INSERT INTO #{@schema}.#{@table_name}
        #{sql_string}"
      end

      def _sql_drop_table_statement
        "DROP TABLE #{@schema}.#{@table_name} CASCADE"
      end

      def _sql_create_table_as_statement(sql_string)
        "CREATE TABLE #{@schema}.#{@table_name} AS
        #{sql_string}"
      end
    end
  end
end
