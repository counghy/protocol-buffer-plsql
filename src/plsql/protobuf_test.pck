CREATE OR REPLACE PACKAGE protobuf_test IS

   -- Author  : SCHMIED.JUERGEN
   -- Created : 10.02.2011 18:38:58
   -- Purpose : 
   PROCEDURE test1;

END protobuf_test;
/
CREATE OR REPLACE PACKAGE BODY protobuf_test IS

   PROCEDURE dump_buffer(p_buffer protobuf_reader.t_buffer) IS
      v_len    PLS_INTEGER := 16;
      v_offset PLS_INTEGER := p_buffer.current_offset;
      v_bytes  RAW(16);
      v_char   PLS_INTEGER;
      v_line   VARCHAR2(128);
   BEGIN
      dbms_output.put_line('----------------------');
      dbms_output.put_line('current_offset=' || p_buffer.current_offset);
      dbms_output.put_line('start_offset=' || p_buffer.start_offset);
      dbms_output.put_line('end_offset=' || p_buffer.end_offset);
   
      WHILE v_len = 16 LOOP
         v_len := least(v_len, p_buffer.end_offset - v_offset);
         EXIT WHEN v_len = 0;
         dbms_lob.READ(p_buffer.buffer, v_len, v_offset, v_bytes);
      
         v_line := 'bytes=' || rpad(rawtohex(v_bytes), 32, ' ') || ' | ';
         FOR i IN 1 .. v_len LOOP
            v_char := utl_raw.cast_to_binary_integer(utl_raw.substr(v_bytes, i, 1));
            IF v_char BETWEEN 32 AND 127 THEN
               v_line := v_line || chr(v_char);
            ELSE
               v_line := v_line || '.';
            END IF;
         END LOOP;
         dbms_output.put_line(v_line);
         v_offset := v_offset + v_len;
      END LOOP;
   END;

   PROCEDURE test1 IS
      v_buffer    protobuf_reader.t_buffer;
      v_adr       protobuf_reader.t_buffer;
      v_anschrift protobuf_reader.t_buffer;
      v_person    protobuf_reader.t_buffer;
      v_blob      BLOB;
   
   BEGIN
   
      SELECT to_blob(hextoraw('0A0D5458303533343234333234323310BF87C28AE12518A78FC28AE125220531303030302A0534373131303208757365726E616D653A0641413130303040B0DCAC0F48E72450015A0D415A2D34333534332D32313231621E0A0B12044A6F686E1A03446F65120F0A064265726C696E12053130333139'))
        INTO v_blob
        FROM dual;
   
      v_buffer := protobuf_reader.init_buffer(v_blob);
      dump_buffer(v_buffer);
   
      dbms_output.put_line(protobuf_reader.read_string(v_buffer, 1));
      dbms_output.put_line(protobuf_reader.read_long_number(v_buffer, 2));
      dbms_output.put_line(protobuf_reader.read_long_number(v_buffer, 3));
      dbms_output.put_line(protobuf_reader.read_string(v_buffer, 4));
      dbms_output.put_line(protobuf_reader.read_string(v_buffer, 5));
      dbms_output.put_line(protobuf_reader.read_string(v_buffer, 6));
      dbms_output.put_line(protobuf_reader.read_string(v_buffer, 7));
      dbms_output.put_line(protobuf_reader.read_number(v_buffer, 8));
      dbms_output.put_line(protobuf_reader.read_number(v_buffer, 9));
      dbms_output.put_line(protobuf_reader.read_number(v_buffer, 10));
      dbms_output.put_line(protobuf_reader.read_string(v_buffer, 11));
   
      v_adr := protobuf_reader.sub_structure_offset(v_buffer, 12);
      dump_buffer(v_adr);
      v_person := protobuf_reader.sub_structure_offset(v_adr, 1);
      dump_buffer(v_person);
   
      dbms_output.put_line('P1 ' || protobuf_reader.read_number(v_person, 1));
      dbms_output.put_line('P2 ' || protobuf_reader.read_string(v_person, 2));
      dbms_output.put_line('P3 ' || protobuf_reader.read_string(v_person, 3));
      dbms_output.put_line('P4 ' || protobuf_reader.read_number(v_person, 4));
   
      v_anschrift := protobuf_reader.sub_structure_offset(v_adr, 2);
      dump_buffer(v_anschrift);
      dbms_output.put_line('A1 ' || protobuf_reader.read_string(v_anschrift, 1));
      dbms_output.put_line('A2 ' || protobuf_reader.read_string(v_anschrift, 2));
      dbms_output.put_line('A3 ' || protobuf_reader.read_string(v_anschrift, 3));
      dbms_output.put_line('A4 ' || protobuf_reader.read_string(v_anschrift, 4));
      dbms_output.put_line('A5 ' || protobuf_reader.read_string(v_anschrift, 5));
      dbms_output.put_line('A6 ' || protobuf_reader.read_string(v_anschrift, 6));
   
   END;

END protobuf_test;
/
