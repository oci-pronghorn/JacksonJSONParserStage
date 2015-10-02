package com.ociweb.pronghorn.adapter.jackson;

import java.io.IOException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.pipe.stream.RingInputStream;
import com.ociweb.pronghorn.pipe.token.TokenBuilder;
import com.ociweb.pronghorn.pipe.token.TypeMask;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

//FOR SINGLE JSON OBJECT DEFINITIONS AND SINGLE MATCHING TEMPLATE
public class JSONReadStage extends PronghornStage {

    
    private final Logger log = LoggerFactory.getLogger(JSONReadStage.class);
    
    private final static int MSG_IDX = 0;
    private final static int MAX_NESTED_JSON_DEPTH = 16;
    
    private InputStream inputStream;
    private Pipe input;
    private JsonFactory jsonFactory;
    private JsonParser jParser;
    private Pipe output;
    
    private FieldReferenceOffsetManager outputFrom;
    
    //use depth to get index into the stack below for the fragment idx
    private int depth;
    private int[] depthFragmentIdxStack = new int[MAX_NESTED_JSON_DEPTH];
    private int[] depthFragmentSeqLenLocStack = new int[MAX_NESTED_JSON_DEPTH];
    private int[] depthFragmentSeqLenStack = new int[MAX_NESTED_JSON_DEPTH];
    
    
    private String rootName;
    private String rootNamePlural;
    private String fieldname;
    private final int maxFragSize;
    private int depthOfRoot = -1;
    
    private boolean specialLiteralAccum = false; //TODO: convert to stack.
    private StringBuffer accumData = new StringBuffer();
    private int accumDataLOC;
    
    
    protected JSONReadStage(GraphManager graphManager, Pipe input, Pipe output) {
        super(graphManager, input, output);
        this.input = input;
        this.output = output;
        this.outputFrom = Pipe.from(output);
        
        if (1 != outputFrom.messageStarts.length) {
            throw new UnsupportedOperationException("Only supports single message, but that message may be made up of multiple fragments");
        }
        assert(outputFrom.messageStarts[0] == MSG_IDX);
                
        maxFragSize = FieldReferenceOffsetManager.maxFragmentSize(outputFrom);
                       
        PipeWriter.setPublishBatchSize(output, 0);
    }
    
    @Override
    public void startup() {
        rootName=outputFrom.fieldNameScript[MSG_IDX];
        rootNamePlural=rootName+'s';
        
        inputStream = new RingInputStream(input);        
        jsonFactory = new JsonFactory(); // or, for data binding, org.codehaus.jackson.mapper.MappingJsonFactory 
        try {
            jParser = jsonFactory.createParser(inputStream);
        } catch (JsonParseException e) {
           throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } // or URL, Stream, Reader, String, byte[]
        
    }
    

    @Override
    public void run() {
        //will exit if there is no more work to do.
        while (true)
        try {
            
            
            if (Pipe.contentRemaining(input)==0) {
                return;//try again later //TODO: this is requried because nextToken hangs when we do not expect it to, shoud investigate.
            }
            
            
            if (!PipeWriter.hasRoomForFragmentOfSize(output, maxFragSize)) {
                return;//try again later when we have room.
            }
                                    
            
            //TOOD: if remaining space can not take largest fragment
            JsonToken token = jParser.nextToken();

            
            if (null==token || JsonToken.NOT_AVAILABLE==token) {
                return;//try again later
            }
            
            //products: [  locations: [    ]      ]
            
            boolean enableLooseRoots = false; //eg multiple roots which are not in an array
            
            switch (token) {
                case START_OBJECT:
                    //TODO: what about top level array products
                    int fragIdx;
                                        
                                                      
                    
                    
                   // String name = outputFrom.fieldNameScript[fragmentIdx];
                    
                    
                    //TODO: MUST get the object name from the stack
                    
                 //   log..println("starting object "+fieldname+" depth "+depth+" objectNAME "+name);
                    
                    //top level objects with no name are root MSG_IDX
                    //or can be part of array named by plural root name.                    
                    if ((enableLooseRoots && depthOfRoot<0 && depth==0) || depthOfRoot == depth) {
                        fragIdx = MSG_IDX;
                        
                    //    log.error("write msg idx "+fragIdx+" ************ try write ");
                        
                        //this starts the new inner fragment  
                        if (!PipeWriter.tryWriteFragment(output, fragIdx)) {
                            throw new UnsupportedOperationException("Expected to have room on output pipe");
                        } 
                        
                        
                    } else {
                    
                        fragIdx = baseFragmentIdx();
                        depthFragmentSeqLenStack[depth]++;
                        
                        //only process if this is not the root wrapper.
                        if (depth>0) {
                        
                       //     log.error("write frag idx "+fragIdx+" ************ try write ");
                            
                          //this starts the new inner fragment  
                          if (!PipeWriter.tryWriteFragment(output, fragIdx)) {
                              throw new UnsupportedOperationException("Expected to have room on output pipe");
                          }                            
                            
                        }          
                        
                    }
                    
//                    

                    
                    break;
                case FIELD_NAME:    
                    fieldname = jParser.getCurrentName();
                 ///   log..println("reading field name "+fieldname);
                    break;
                case VALUE_FALSE:
                     
               //     we are using the wrong msg_idx we must look this up from stack!!!
                    //TODO: add safety check if lookup is crosses a boundry into another fragment!!!
                    
                    switch (TokenBuilder.extractType(FieldReferenceOffsetManager.lookupToken(fieldname, baseFragmentIdx(), outputFrom))) {
                        case TypeMask.IntegerUnsigned:
                        case TypeMask.IntegerUnsignedOptional:
                            PipeWriter.writeInt(output, FieldReferenceOffsetManager.lookupFieldLocator(fieldname, baseFragmentIdx(), outputFrom), 0);
                            break;                            
                        case TypeMask.TextASCII:
                            PipeWriter.writeASCII(output, FieldReferenceOffsetManager.lookupFieldLocator(fieldname, baseFragmentIdx(), outputFrom), jParser.getValueAsString(""));
                            break;
                        case TypeMask.TextASCIIOptional:
                            //TODO: how do we write a null?
                            PipeWriter.writeASCII(output, FieldReferenceOffsetManager.lookupFieldLocator(fieldname, baseFragmentIdx(), outputFrom), jParser.getValueAsString(""));
                            break;    
                            
                        default:
                            throw new UnsupportedOperationException(TokenBuilder.tokenToString(FieldReferenceOffsetManager.lookupToken(fieldname, baseFragmentIdx(), outputFrom)));
                             
                    }
                    
                    break;
                case VALUE_TRUE:    
                    switch (TokenBuilder.extractType(FieldReferenceOffsetManager.lookupToken(fieldname, baseFragmentIdx(), outputFrom))) {
                        case TypeMask.IntegerUnsigned:
                        case TypeMask.IntegerUnsignedOptional:
                            PipeWriter.writeInt(output, FieldReferenceOffsetManager.lookupFieldLocator(fieldname, baseFragmentIdx(), outputFrom), 1);
                            break;                            
                        case TypeMask.TextASCII:
                            PipeWriter.writeASCII(output, FieldReferenceOffsetManager.lookupFieldLocator(fieldname, baseFragmentIdx(), outputFrom), jParser.getValueAsString(""));
                            break;
                        case TypeMask.TextASCIIOptional:
                            PipeWriter.writeASCII(output, FieldReferenceOffsetManager.lookupFieldLocator(fieldname, baseFragmentIdx(), outputFrom), jParser.getValueAsString(""));
                            break;    
                        
                        default:
                            throw new UnsupportedOperationException(TokenBuilder.tokenToString(FieldReferenceOffsetManager.lookupToken(fieldname, MSG_IDX /* get from stack */, outputFrom)));
                         
                    }
                    break;
                case VALUE_NUMBER_FLOAT:
                    System.out.println("FLOAT "+fieldname+" with value "+jParser.getValueAsString(""));
                    PipeWriter.writeFloatAsIntBits(output, FieldReferenceOffsetManager.lookupFieldLocator(fieldname, baseFragmentIdx(), outputFrom), jParser.getFloatValue());
                    break;
                case VALUE_NUMBER_INT:
                    switch (TokenBuilder.extractType(FieldReferenceOffsetManager.lookupToken(fieldname, baseFragmentIdx(), outputFrom))) {
                        case TypeMask.IntegerUnsigned:
                        case TypeMask.IntegerUnsignedOptional:
                        case TypeMask.IntegerSigned:
                        case TypeMask.IntegerSignedOptional:
                            PipeWriter.writeInt(output, FieldReferenceOffsetManager.lookupFieldLocator(fieldname, baseFragmentIdx(), outputFrom), (int)jParser.getLongValue());
                        break;                            
                        case TypeMask.LongUnsigned:
                        case TypeMask.LongUnsignedOptional:
                        case TypeMask.LongSigned:
                        case TypeMask.LongSignedOptional:
                            PipeWriter.writeLong(output, FieldReferenceOffsetManager.lookupFieldLocator(fieldname, baseFragmentIdx(), outputFrom), jParser.getLongValue());
                        break;
                        
                        default:
                            throw new UnsupportedOperationException(TokenBuilder.tokenToString(FieldReferenceOffsetManager.lookupToken(fieldname, MSG_IDX /* get from stack */, outputFrom)));                         
                    }
                    break;
                case VALUE_STRING:
                    if (specialLiteralAccum) {
                        if (accumData.length()>0) {
                            accumData.append(',');
                        }
                        accumData.append(jParser.getValueAsString(""));
                    } else { 
                        
                        switch (TokenBuilder.extractType(FieldReferenceOffsetManager.lookupToken(fieldname, baseFragmentIdx(), outputFrom))) {
                            case TypeMask.IntegerUnsigned:
                            case TypeMask.IntegerUnsignedOptional:
                            case TypeMask.IntegerSigned:
                            case TypeMask.IntegerSignedOptional:
                                PipeWriter.writeInt(output, FieldReferenceOffsetManager.lookupFieldLocator(fieldname, baseFragmentIdx(), outputFrom), (int)Long.parseLong(jParser.getValueAsString("0")));
                            break;
                            case TypeMask.LongUnsigned:
                            case TypeMask.LongUnsignedOptional:
                            case TypeMask.LongSigned:
                            case TypeMask.LongSignedOptional:
                                PipeWriter.writeLong(output, FieldReferenceOffsetManager.lookupFieldLocator(fieldname, baseFragmentIdx(), outputFrom), Long.parseLong(jParser.getValueAsString("0")));
                            break;  
                            case TypeMask.TextASCII:
                            case TypeMask.TextASCIIOptional:
                                PipeWriter.writeASCII(output, FieldReferenceOffsetManager.lookupFieldLocator(fieldname, baseFragmentIdx(), outputFrom), jParser.getValueAsString(""));
                            break;
                            case TypeMask.TextUTF8:
                            case TypeMask.TextUTF8Optional:
                                PipeWriter.writeUTF8(output, FieldReferenceOffsetManager.lookupFieldLocator(fieldname, baseFragmentIdx(), outputFrom), jParser.getValueAsString(""));
                            break;    
                            
                            default:
                                throw new UnsupportedOperationException(TokenBuilder.tokenToString(FieldReferenceOffsetManager.lookupToken(fieldname, baseFragmentIdx(), outputFrom)));                         
                        }    
                        
                    }
                    break;                
                case START_ARRAY:
                    
                    int wrappingFragmentIdx = baseFragmentIdx(); //must happen before we inc the depth
                    
                    //TODO: start fresh count
                    //starts sequence
                    depth++;
                    
          
                    
                    //log..println("start array "+fieldname);
                    
                    if (rootNamePlural.equals(fieldname)) {
                        //top level detected.
                        if (depthOfRoot>=0) {
                            throw new UnsupportedOperationException("The root name "+rootNamePlural+" may only be used once at the top.");
                        }
                        depthOfRoot = depth;
                    } else {
                                                
                        //TODO: Need to establish plural or not and simplify logic
                        String proposedSquenceName = fieldname.endsWith("s") ? fieldname.substring(0,fieldname.length()-1) :  fieldname;
                        
                        int tempFragIdx = FieldReferenceOffsetManager.lookupFragmentLocator(proposedSquenceName, wrappingFragmentIdx, outputFrom);

                        if (FieldReferenceOffsetManager.isGroupSequence(outputFrom, tempFragIdx)) {
                            
                            int sequenceLengthLoc = FieldReferenceOffsetManager.lookupSequenceLengthLoc("No"+proposedSquenceName, wrappingFragmentIdx, outputFrom);
                                              
                            int tokenFound = FieldReferenceOffsetManager.lookupToken("No"+proposedSquenceName, wrappingFragmentIdx, outputFrom);
                            System.err.println("len token found as "+TokenBuilder.tokenToString(tokenFound));
                            
                            
                            //cursor is right however it is stateful and requires the stack to be at the right position.
                            depthFragmentIdxStack[depth] = tempFragIdx; //hold this so we can start the fragment for each new object
                            depthFragmentSeqLenLocStack[depth] = sequenceLengthLoc;   
                                                        
                            depthFragmentSeqLenStack[depth] = 0;
                            
                            //do not publish here because we do not have the count of the sequence length yet.
                            //TODO: this breaks the byte ring buffer boundary info???
                            
                        } else {
                            //this is an array in JSON but not a sequence in FAST 
                            //right now we just support ASCII array so append the data as it comes in until.
                            accumDataLOC = FieldReferenceOffsetManager.lookupFieldLocator(proposedSquenceName, wrappingFragmentIdx, outputFrom);
                            specialLiteralAccum = true;
                            accumData.setLength(0);
                        }
                        
                        
                    }
                    
                    break;
                    
                case END_ARRAY:   
                    
                    if (specialLiteralAccum) {
                        //Write but TODO: this can be optimized by early writes isntead of this late write
                       // log.error("close  "+accumData);
                        
                        PipeWriter.writeASCII(output, accumDataLOC, accumData.toString());
                        
                        accumData.setLength(0);
                        specialLiteralAccum = false;
                    } else {
                        if (depth>depthOfRoot) {
                            int lenLOC = depthFragmentSeqLenLocStack[depth]; //TODO: this writes to the wrong place!! see example code
                            int count = depthFragmentSeqLenStack[depth];
                                                        
                            
                            log.error("total count in sequence "+count+" depth "+depth+" "+depthOfRoot+" wrote to position "+lenLOC);
                            PipeWriter.writeInt(output, lenLOC+2, count); //TODO: AAA, Why does this require +2  What is wrong!!!!!!
                        }
                        
                    }
                    
                    //TODO: record the count
                    //end sequence
                    depth--;
                    break;
                case END_OBJECT:    
                    if (depth==depthOfRoot) {
                         PipeWriter.publishWrites(output); //do not publish until we resolve length of the sequence.
                         log.error("after publish "+output);
                    }
                    break;
                case VALUE_NULL:
                    throw new UnsupportedOperationException("Null needs a type");
                default:
                    throw new UnsupportedOperationException();
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }       
               
        
        
    }

    private int baseFragmentIdx() {
        return depthFragmentIdxStack[depth];
    }
    
    @Override
    public void shutdown() {
        try {
            Pipe.publishEOF(output); //FAST stage requires posion pill, TODO: woudl be nice to remvoe this requirement.
            jParser.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    

}
