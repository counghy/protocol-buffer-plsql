CREATE OR REPLACE PACKAGE protobuf_writer IS

   -- 2012-05-10 first version by juergen schmied
   --
   -- Writing Google protocol buffers
   --
   --  Example:
   --
   --  message sub_structure {
   --    optional int32  field1 = 1;
   --    optional string field2 = 2;
   --    optional string field3 = 3;
   --  }
   --
   -- message structure {
   --  optional sub_structure = 3;
   -- }
   --
   --  v_raw := protobuf_writer.create_structure(3,
   --    utl_raw.concat(
   --      protobuf_writer.create_uint32(1, v_field1),
   --      protobuf_writer.create_string(2, v_field2),
   --      protobuf_writer.create_string(3, v_field3)
   --    )
   --  )
   --

   -- internal
   --FUNCTION to_varint64(p_value INTEGER) RETURN RAW;
   --FUNCTION to_varint32(p_value INTEGER) RETURN RAW;

   FUNCTION create_structure(p_field_index PLS_INTEGER, p_payload RAW) RETURN RAW;
   FUNCTION create_string(p_field_index PLS_INTEGER, p_string VARCHAR2) RETURN RAW;
   FUNCTION create_uint32(p_field_index PLS_INTEGER, p_value INTEGER) RETURN RAW;
   FUNCTION create_uint64(p_field_index PLS_INTEGER, p_value INTEGER) RETURN RAW;

END protobuf_writer;
/
CREATE OR REPLACE PACKAGE BODY protobuf_writer IS

   c_zero_mask         RAW(1) := hextoraw('00');
   c_end_flag_mask     RAW(1) := hextoraw('80');
   c_used_bits_mask    RAW(1) := hextoraw('7f');
   c_overflow_mask     RAW(1) := hextoraw('70');
   c_lower_4_bits_mask RAW(1) := hextoraw('0f');
   c_lower_1_bits_mask RAW(1) := hextoraw('01');

   c_shift_7  BINARY_INTEGER := 128;
   c_shift_14 BINARY_INTEGER := 128 * 128;
   c_shift_21 BINARY_INTEGER := 128 * 128 * 128;
   c_shift_28 BINARY_INTEGER := 128 * 128 * 128 * 128;

   c_data_varint           BINARY_INTEGER := 0;
   c_data_64bit            BINARY_INTEGER := 1;
   c_data_length_delimited BINARY_INTEGER := 2;
   c_data_start_group      BINARY_INTEGER := 3;
   c_data_end_group        BINARY_INTEGER := 4;
   c_data_32bit            BINARY_INTEGER := 5;

   --
   -- unsigned int
   -- 64 bit are 7*9 + 1 bit -> 10 byte
   --
   FUNCTION to_varint64(p_value INTEGER) RETURN RAW IS
      v_digit  BINARY_INTEGER;
      v_byte   RAW(1);
      v_result RAW(19);
      v_value  INTEGER := p_value;
   BEGIN
      FOR i IN 1 .. 9 LOOP
         v_digit := v_value MOD c_shift_7;
         v_value := floor(v_value / c_shift_7);
      
         v_byte := utl_raw.substr(utl_raw.cast_from_binary_integer(v_digit, utl_raw.little_endian), 1, 1);
      
         IF v_value != 0 THEN
            v_byte := utl_raw.bit_or(v_byte, c_end_flag_mask);
         END IF;
      
         v_result := utl_raw.concat(v_result, v_byte);
      
         IF v_value = 0 THEN
            RETURN v_result;
         END IF;
      END LOOP;
   
      IF NOT v_value BETWEEN - 1 AND 1 THEN
         raise_application_error(-20000, 'value not in range -2^63..2^64-1');
      END IF;
   
      -- last byte with the remaining bit
      v_byte   := utl_raw.substr(utl_raw.cast_from_binary_integer(v_value, utl_raw.little_endian), 1, 1);
      v_result := utl_raw.concat(v_result, utl_raw.bit_and(v_byte, c_lower_1_bits_mask));
   
      RETURN v_result;
   END;

   --
   -- unsigned int
   -- 32 bit are 7*4 + 4 bit -> 5 byte
   --
   FUNCTION to_varint32(p_value INTEGER) RETURN RAW IS
      v_digit  BINARY_INTEGER;
      v_byte   RAW(1);
      v_result RAW(5);
      v_value  INTEGER := p_value;
   BEGIN
      FOR i IN 1 .. 4 LOOP
         v_digit := v_value MOD c_shift_7;
         v_value := floor(v_value / c_shift_7);
      
         v_byte := utl_raw.substr(utl_raw.cast_from_binary_integer(v_digit, utl_raw.little_endian), 1, 1);
      
         IF v_value != 0 THEN
            v_byte := utl_raw.bit_or(v_byte, c_end_flag_mask);
         END IF;
      
         v_result := utl_raw.concat(v_result, v_byte);
      
         IF v_value = 0 THEN
            RETURN v_result;
         END IF;
      END LOOP;
   
      IF NOT v_value BETWEEN - 8 AND 15 THEN
         raise_application_error(-20000, 'value not in range -2^31..2^32-1');
      END IF;
   
      -- last byte with the remaining bit
      v_byte   := utl_raw.substr(utl_raw.cast_from_binary_integer(v_value, utl_raw.little_endian), 1, 1);
      v_result := utl_raw.concat(v_result, utl_raw.bit_and(v_byte, c_lower_4_bits_mask));
   
      RETURN v_result;
   END;

   FUNCTION create_structure(p_field_index PLS_INTEGER, p_payload RAW) RETURN RAW IS
      v_key    RAW(5);
      v_length RAW(5);
   BEGIN
      v_key    := to_varint32(c_data_length_delimited + p_field_index * 8);
      v_length := to_varint32(utl_raw.length(p_payload));
      RETURN utl_raw.concat(v_key, v_length, p_payload);
   END;

   FUNCTION create_string(p_field_index PLS_INTEGER, p_string VARCHAR2) RETURN RAW IS
      v_key    RAW(5);
      v_length RAW(5);
      v_string RAW(2000);
   BEGIN
      IF p_string IS NOT NULL THEN
         v_key    := to_varint32(c_data_length_delimited + p_field_index * 8);
         v_string := utl_i18n.string_to_raw(p_string, 'utf8');
         v_length := to_varint32(utl_raw.length(v_string));
         RETURN utl_raw.concat(v_key, v_length, v_string);
      ELSE
         RETURN NULL;
      END IF;
   END;

   FUNCTION create_uint32(p_field_index PLS_INTEGER, p_value INTEGER) RETURN RAW IS
      v_key   RAW(5);
      v_value RAW(5);
   BEGIN
      v_key   := to_varint32(c_data_varint + p_field_index * 8);
      v_value := to_varint32(p_value);
      RETURN utl_raw.concat(v_key, v_value);
   END;

   FUNCTION create_uint64(p_field_index PLS_INTEGER, p_value INTEGER) RETURN RAW IS
      v_key   RAW(5);
      v_value RAW(10);
   BEGIN
      v_key   := to_varint32(c_data_varint + p_field_index * 8);
      v_value := to_varint64(p_value);
      RETURN utl_raw.concat(v_key, v_value);
   END;

END protobuf_writer;
/
