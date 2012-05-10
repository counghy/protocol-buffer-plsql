CREATE OR REPLACE PACKAGE protobuf_reader IS

   -- Author  : SCHMIED.JUERGEN
   -- Created : 18.01.2011 19:26:07
   -- Version:  10.05.2012
   -- Purpose : 

   /*
   
   Beispiel:
        
         message myMessage { 
           required int32 firstID = 1;
           optional string firstString  = 2;
           enum myenum {
             CHOICE1 = 0;
             CHOICE2 = 1;
             CHOICE3 = 2;
           }
           required  myenum myChoice = 3 [default = CHOICE1];
           message event {
             required int32 id = 1;
             optional string description = 2;
           }
           repeated EreignisType event = 4;
         }
         
   
         v_buffer     := protobuf_reader.init_buffer(v_blob);
       
         dbms_output.put_line(protobuf_reader.read_number(v_buffer, 1));
         dbms_output.put_line(protobuf_reader.read_string(v_buffer, 2));
         dbms_output.put_line(protobuf_reader.read_number(v_buffer, 3));
       
         v_sub_buffer := protobuf_reader.sub_structure_offset(v_buffer, 4);
         dbms_output.put_line(protobuf_reader.read_number(v_sub_buffer, 1));
         dbms_output.put_line(protobuf_reader.read_string(v_sub_buffer, 2));
       
         v_sub_buffer := protobuf_reader.sub_structure_offset(v_buffer, 4);
         dbms_output.put_line(protobuf_reader.read_number(v_sub_buffer, 1));
         dbms_output.put_line(protobuf_reader.read_string(v_sub_buffer, 2));
         
         Every read moves the current position inside the buffer, so the fields must be read 
         sequentiell or you have to call protobuf_reader.init_buffer again or work with a copy
         of t_buffer.
         
         The same applies to protobuf_reader.sub_structure_offset so you must give a index of 1 
         to read the next structure.
         
     -- internal functions
     FUNCTION readrawvarint32(p_buffer IN OUT t_buffer) RETURN BINARY_INTEGER;
     FUNCTION readrawstring(p_buffer IN OUT t_buffer) RETURN VARCHAR2;
   */
   TYPE t_buffer IS RECORD(
      buffer         BLOB,
      current_offset PLS_INTEGER,
      start_offset   PLS_INTEGER,
      end_offset     PLS_INTEGER);

   FUNCTION init_buffer(p_buffer BLOB) RETURN t_buffer;
   FUNCTION sub_structure_offset(p_buffer IN OUT t_buffer, p_field_number IN PLS_INTEGER, p_index IN PLS_INTEGER := 1) RETURN t_buffer;
   FUNCTION read_number(p_buffer IN OUT t_buffer, p_field_number IN PLS_INTEGER, p_index IN PLS_INTEGER := 1) RETURN NUMBER;
   FUNCTION read_long_number(p_buffer IN OUT t_buffer, p_field_number IN PLS_INTEGER, p_index IN PLS_INTEGER := 1) RETURN NUMBER;
   FUNCTION read_float(p_buffer IN OUT t_buffer, p_field_number IN PLS_INTEGER, p_index IN PLS_INTEGER := 1) RETURN NUMBER;
   FUNCTION read_string(p_buffer IN OUT t_buffer, p_field_number IN PLS_INTEGER, p_index IN PLS_INTEGER := 1) RETURN VARCHAR2;

END protobuf_reader;
/
CREATE OR REPLACE PACKAGE BODY protobuf_reader IS

   c_zero_mask         RAW(1) := hextoraw('00');
   c_end_flag_mask     RAW(1) := hextoraw('80');
   c_used_bits_mask    RAW(1) := hextoraw('7f');
   c_overflow_mask     RAW(1) := hextoraw('70');
   c_lower_4_bits_mask RAW(1) := hextoraw('0f');

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

   FUNCTION init_buffer(p_buffer BLOB) RETURN t_buffer IS
      v_buffer t_buffer;
   BEGIN
      v_buffer.buffer         := p_buffer;
      v_buffer.start_offset   := 1;
      v_buffer.current_offset := 1;
      v_buffer.end_offset     := dbms_lob.getlength(p_buffer);
      RETURN v_buffer;
   END;

   /**
   * Read a raw Varint from the stream.  If larger than 32 bits, throws a exception
   */
   FUNCTION readrawvarint32(p_buffer IN OUT t_buffer) RETURN BINARY_INTEGER IS
      v_byte   RAW(1);
      v_len    PLS_INTEGER := 1;
      v_result BINARY_INTEGER;
   BEGIN
      dbms_lob.read(p_buffer.buffer, v_len, p_buffer.current_offset, v_byte);
      p_buffer.current_offset := p_buffer.current_offset + v_len;
   
      v_result := utl_raw.cast_to_binary_integer(utl_raw.bit_and(v_byte, c_used_bits_mask));
   
      IF utl_raw.bit_and(v_byte, c_end_flag_mask) = c_end_flag_mask THEN
      
         dbms_lob.read(p_buffer.buffer, v_len, p_buffer.current_offset, v_byte);
         p_buffer.current_offset := p_buffer.current_offset + v_len;
         v_result                := v_result + utl_raw.cast_to_binary_integer(utl_raw.bit_and(v_byte, c_used_bits_mask)) * c_shift_7;
      
         IF utl_raw.bit_and(v_byte, c_end_flag_mask) = c_end_flag_mask THEN
         
            dbms_lob.read(p_buffer.buffer, v_len, p_buffer.current_offset, v_byte);
            p_buffer.current_offset := p_buffer.current_offset + v_len;
            v_result                := v_result + utl_raw.cast_to_binary_integer(utl_raw.bit_and(v_byte, c_used_bits_mask)) * c_shift_14;
         
            IF utl_raw.bit_and(v_byte, c_end_flag_mask) = c_end_flag_mask THEN
            
               dbms_lob.read(p_buffer.buffer, v_len, p_buffer.current_offset, v_byte);
               p_buffer.current_offset := p_buffer.current_offset + v_len;
               v_result                := v_result + utl_raw.cast_to_binary_integer(utl_raw.bit_and(v_byte, c_used_bits_mask)) * c_shift_21;
            
               IF utl_raw.bit_and(v_byte, c_end_flag_mask) = c_end_flag_mask THEN
               
                  dbms_lob.read(p_buffer.buffer, v_len, p_buffer.current_offset, v_byte);
                  p_buffer.current_offset := p_buffer.current_offset + v_len;
               
                  IF utl_raw.bit_and(v_byte, c_overflow_mask) != c_zero_mask THEN
                     raise_application_error(-20000, 'readrawvarint32 overflows - use readrawvarint64 instead');
                  END IF;
               
                  -- use only the lower 4 bits - BINARY_INTEGER would overflow otherwise
                  v_result := v_result + utl_raw.cast_to_binary_integer(utl_raw.bit_and(v_byte, c_lower_4_bits_mask)) * c_shift_28;
               
                  IF utl_raw.bit_and(v_byte, c_end_flag_mask) = c_end_flag_mask THEN
                     raise_application_error(-20000, 'readrawvarint32 overflows - use readrawvarint64 instead');
                  END IF;
               END IF;
            END IF;
         END IF;
      END IF;
   
      RETURN v_result;
   END;

   FUNCTION readrawvarint64(p_buffer IN OUT t_buffer) RETURN INTEGER IS
      v_byte   RAW(1);
      v_len    PLS_INTEGER := 1;
      v_result INTEGER := 0;
      v_shift  INTEGER := 1;
   BEGIN
      FOR i IN 1 .. 10 LOOP
         dbms_lob.read(p_buffer.buffer, v_len, p_buffer.current_offset, v_byte);
         p_buffer.current_offset := p_buffer.current_offset + v_len;
         v_result                := v_result + utl_raw.cast_to_binary_integer(utl_raw.bit_and(v_byte, c_used_bits_mask)) * v_shift;
      
         IF utl_raw.bit_and(v_byte, c_end_flag_mask) != c_end_flag_mask THEN
            RETURN v_result;
         END IF;
      
         v_shift := v_shift * c_shift_7;
      END LOOP;
   
      raise_application_error(-20000, 'malformed Varint');
   END;

   PROCEDURE skiprawvarint(p_buffer IN OUT t_buffer) IS
      v_byte RAW(1);
      v_len  PLS_INTEGER := 1;
   BEGIN
      FOR i IN 1 .. 10 LOOP
         dbms_lob.read(p_buffer.buffer, v_len, p_buffer.current_offset, v_byte);
         p_buffer.current_offset := p_buffer.current_offset + v_len;
      
         IF utl_raw.bit_and(v_byte, c_end_flag_mask) != c_end_flag_mask THEN
            RETURN;
         END IF;
      END LOOP;
   
      raise_application_error(-20000, 'malformed Varint');
   END;

   FUNCTION readrawstring(p_buffer IN OUT t_buffer) RETURN VARCHAR2 IS
      v_len    PLS_INTEGER;
      v_buffer RAW(256);
   BEGIN
      v_len := readrawvarint32(p_buffer);
      IF v_len > 0 THEN
         dbms_lob.read(p_buffer.buffer, v_len, p_buffer.current_offset, v_buffer);
      END IF;
      p_buffer.current_offset := p_buffer.current_offset + v_len;
      RETURN utl_i18n.raw_to_char(v_buffer, 'utf8');
   END;

   PROCEDURE skiprawstring(p_buffer IN OUT t_buffer) IS
   BEGIN
      p_buffer.current_offset := p_buffer.current_offset + readrawvarint32(p_buffer);
   END;

   FUNCTION readfixed32float(p_buffer IN OUT t_buffer) RETURN INTEGER IS
      v_len    PLS_INTEGER;
      v_buffer RAW(4);
   BEGIN
      v_len := 4;
      dbms_lob.read(p_buffer.buffer, v_len, p_buffer.current_offset, v_buffer);
      p_buffer.current_offset := p_buffer.current_offset + 4;
      RETURN utl_raw.cast_to_binary_float(v_buffer, utl_raw.little_endian);
   END;

   PROCEDURE skipfixed32(p_buffer IN OUT t_buffer) IS
   BEGIN
      p_buffer.current_offset := p_buffer.current_offset + 4;
   END;

   PROCEDURE skip_field(p_buffer IN OUT t_buffer, p_data_type PLS_INTEGER) IS
   BEGIN
      CASE p_data_type
         WHEN c_data_varint THEN
            protobuf_reader.skiprawvarint(p_buffer);
         WHEN c_data_length_delimited THEN
            protobuf_reader.skiprawstring(p_buffer);
         WHEN c_data_32bit THEN
            protobuf_reader.skipfixed32(p_buffer);
         ELSE
            raise_application_error(-20000, 'typ ' || p_data_type || ' not implemented');
      END CASE;
   END;

   FUNCTION sub_structure_offset(p_buffer IN OUT t_buffer, p_field_number IN PLS_INTEGER, p_index IN PLS_INTEGER := 1) RETURN t_buffer IS
      v_current_int INTEGER;
      v_data_type   PLS_INTEGER;
      v_field_nr    PLS_INTEGER;
      v_index       PLS_INTEGER := 1;
      v_sub_buffer  t_buffer;
   BEGIN
      WHILE p_buffer.current_offset < p_buffer.end_offset LOOP
         v_current_int := protobuf_reader.readrawvarint32(p_buffer);
         v_data_type   := v_current_int MOD 8;
         v_field_nr    := v_current_int / 8;
      
         IF p_field_number = v_field_nr THEN
            IF v_data_type != c_data_length_delimited THEN
               raise_application_error(-20000,
                                       'the  value on field index=' || p_field_number || ' is not a ''Lengthdelimited'' but of type=' ||
                                       v_data_type);
            END IF;
         
            IF v_index = p_index THEN
               v_current_int               := protobuf_reader.readrawvarint32(p_buffer);
               v_sub_buffer.buffer         := p_buffer.buffer;
               v_sub_buffer.start_offset   := p_buffer.current_offset;
               v_sub_buffer.current_offset := p_buffer.current_offset;
               v_sub_buffer.end_offset     := p_buffer.current_offset + v_current_int;
               p_buffer.current_offset     := p_buffer.current_offset + v_current_int;
               RETURN v_sub_buffer;
            ELSE
               v_index := v_index + 1;
            END IF;
         END IF;
      
         skip_field(p_buffer, v_data_type);
      END LOOP;
   
      RETURN NULL;
   END;

   FUNCTION read_number(p_buffer IN OUT t_buffer, p_field_number IN PLS_INTEGER, p_index IN PLS_INTEGER := 1) RETURN NUMBER IS
      v_current_int  INTEGER;
      v_data_type    PLS_INTEGER;
      v_field_nr     PLS_INTEGER;
      v_index        PLS_INTEGER := 1;
      v_old_position PLS_INTEGER := p_buffer.current_offset;
   BEGIN
      WHILE p_buffer.current_offset < p_buffer.end_offset LOOP
         v_current_int := protobuf_reader.readrawvarint32(p_buffer);
         v_data_type   := v_current_int MOD 8;
         v_field_nr    := v_current_int / 8;
      
         IF p_field_number = v_field_nr THEN
            IF v_index = p_index THEN
               IF v_data_type != c_data_varint THEN
                  raise_application_error(-20000,
                                          'the  value on field index=' || p_field_number || ' is not a Varint but of type=' || v_data_type);
               END IF;
               RETURN readrawvarint64(p_buffer);
            ELSE
               v_index := v_index + 1;
            END IF;
         END IF;
      
         skip_field(p_buffer, v_data_type);
      END LOOP;
      p_buffer.current_offset := v_old_position;
      RETURN NULL;
   END;

   FUNCTION read_long_number(p_buffer IN OUT t_buffer, p_field_number IN PLS_INTEGER, p_index IN PLS_INTEGER := 1) RETURN NUMBER IS
      v_current_int  INTEGER;
      v_data_type    PLS_INTEGER;
      v_field_nr     PLS_INTEGER;
      v_index        PLS_INTEGER := 1;
      v_old_position PLS_INTEGER := p_buffer.current_offset;
   BEGIN
      WHILE p_buffer.current_offset < p_buffer.end_offset LOOP
         v_current_int := protobuf_reader.readrawvarint64(p_buffer);
         v_data_type   := v_current_int MOD 8;
         v_field_nr    := v_current_int / 8;
      
         IF p_field_number = v_field_nr THEN
            IF v_index = p_index THEN
               IF v_data_type != c_data_varint THEN
                  raise_application_error(-20000,
                                          'the  value on field index=' || p_field_number || ' is not a Varint but of type=' || v_data_type);
               END IF;
               RETURN readrawvarint64(p_buffer);
            ELSE
               v_index := v_index + 1;
            END IF;
         END IF;
      
         skip_field(p_buffer, v_data_type);
      END LOOP;
   
      p_buffer.current_offset := v_old_position;
      RETURN NULL;
   END;

   FUNCTION read_float(p_buffer IN OUT t_buffer, p_field_number IN PLS_INTEGER, p_index IN PLS_INTEGER := 1) RETURN NUMBER IS
      v_current_int  INTEGER;
      v_data_type    PLS_INTEGER;
      v_field_nr     PLS_INTEGER;
      v_index        PLS_INTEGER := 1;
      v_old_position PLS_INTEGER := p_buffer.current_offset;
   BEGIN
      WHILE p_buffer.current_offset < p_buffer.end_offset LOOP
         v_current_int := protobuf_reader.readrawvarint32(p_buffer);
         v_data_type   := v_current_int MOD 8;
         v_field_nr    := v_current_int / 8;
      
         IF p_field_number = v_field_nr THEN
            IF v_index = p_index THEN
               IF v_data_type != c_data_32bit THEN
                  raise_application_error(-20000,
                                          'the  value on field index==' || p_field_number || ' is not a fixed32, sfixed32 or float');
               END IF;
               RETURN readfixed32float(p_buffer);
            ELSE
               v_index := v_index + 1;
            END IF;
         END IF;
      
         skip_field(p_buffer, v_data_type);
      END LOOP;
      p_buffer.current_offset := v_old_position;
      RETURN NULL;
   END;

   FUNCTION read_string(p_buffer IN OUT t_buffer, p_field_number IN PLS_INTEGER, p_index IN PLS_INTEGER := 1) RETURN VARCHAR2 IS
      v_current_int  INTEGER;
      v_data_type    PLS_INTEGER;
      v_field_nr     PLS_INTEGER;
      v_index        PLS_INTEGER := 1;
      v_old_position PLS_INTEGER := p_buffer.current_offset;
   BEGIN
      WHILE p_buffer.current_offset < p_buffer.end_offset LOOP
         v_current_int := protobuf_reader.readrawvarint32(p_buffer);
         v_data_type   := v_current_int MOD 8;
         v_field_nr    := v_current_int / 8;
      
         IF p_field_number = v_field_nr THEN
            IF v_index = p_index THEN
               IF v_data_type != c_data_length_delimited THEN
                  raise_application_error(-20000, 'the  value on field index==' || p_field_number || ' is not a ''Lengthdelimited''');
               END IF;
               RETURN readrawstring(p_buffer);
            ELSE
               v_index := v_index + 1;
            END IF;
         END IF;
      
         skip_field(p_buffer, v_data_type);
      END LOOP;
   
      p_buffer.current_offset := v_old_position;
      RETURN NULL;
   END;

END protobuf_reader;
/
