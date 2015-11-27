#Example for a view with fields from a protobuffer

```
CREATE OR REPLACE FUNCTION protoread_number(v_lob BLOB, v_idx PLS_INTEGER) RETURN VARCHAR2 IS
   v_buffer protobuf_reader.t_buffer;
BEGIN
   v_buffer := protobuf_reader.init_buffer(v_lob);
   RETURN protobuf_reader.read_long_number(v_buffer, v_idx);
END;
```

```
CREATE OR REPLACE FUNCTION protoread_string(v_lob BLOB, v_idx PLS_INTEGER) RETURN VARCHAR2 IS
   v_buffer protobuf_reader.t_buffer;
BEGIN
   v_buffer := protobuf_reader.init_buffer(v_lob);
   RETURN protobuf_reader.read_string(v_buffer, v_idx);
END ;
```

```
CREATE OR REPLACE VIEW V_PROTO AS
SELECT protoread_number(e.protobuf, 1) AS id1,
       protoread_string(e.protobuf, 2) AS id2
  FROM ereignis;
```