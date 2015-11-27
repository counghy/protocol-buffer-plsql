# Example (reading) #

```
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

  repeated event eventType = 4;
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
```

Every read moves the current position inside the buffer, so the fields must be read sequentiell or you have to call rotobuf\_reader.init\_buffer again or work with a copy of t\_buffer.


The same applies to protobuf\_reader.sub\_structure\_offset so you must give a index of 1 to read the next structure.

# Example (writing) #
```
v_raw := protobuf_writer.create_structure(3,
  utl_raw.concat(
    protobuf_writer.create_uint32(1, v_field1),
    protobuf_writer.create_string(2, v_field2),
    protobuf_writer.create_string(3, v_field3)));
```