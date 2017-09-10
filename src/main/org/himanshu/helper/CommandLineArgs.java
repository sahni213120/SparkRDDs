package org.himanshu.helper;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.IOException;

/**
 * Created by himanshu on 6/4/2017.
 */
public class CommandLineArgs {

    public CommandLineArgs(String[] args) throws IOException {
        parseArgs(args);
    }

    @Option(name = "--sourceLocation",usage = "location of source file" , required = false)
    private String sourceLocation;

    @Option(name = "--masterLocation",usage = "location of master file", required = false)
    private String masterLocation;

    @Option(name = "--validFileLocation",usage = "location of target file", required = true )
    private String validLocation;

    @Option(name = "--errorFileLocation",usage = "location of target file", required = false )
    private String errorLocation;

    @Option(name = "--metaFileLocation", usage = "location of meta (json file)",required = true)
    private String metaFileLocation;

    public String getSourceLocation() {
        return sourceLocation;
    }

    public void setSourceLocation(String sourceLocation) {
        this.sourceLocation = sourceLocation;
    }

    public String getMasterLocation() {
        return masterLocation;
    }

    public void setMasterLocation(String masterLocation) {
        this.masterLocation = masterLocation;
    }

    public String getValidLocation() {
        return validLocation;
    }

    public void setValidLocation(String targetLocation) {
        this.validLocation = targetLocation;
    }

    public String getErrorLocation() {
        return errorLocation;
    }

    public void setErrorLocation(String errorLocation) {
        this.errorLocation = errorLocation;
    }

    public String getMetaFileLocation() {
        return metaFileLocation;
    }

    public void setMetaFileLocation(String metaFileLocation) {
        this.metaFileLocation = metaFileLocation;
    }

    private void parseArgs(String[] args) throws IOException {
        CmdLineParser parser = new CmdLineParser(this);
        try {
            parser.parseArgument(args);
        } catch( CmdLineException e ) {
            int start = e.getMessage().indexOf('"')+1;
            int end   = e.getMessage().lastIndexOf('"');
            String wrongArgument = e.getMessage().substring(start, end);
            System.err.println("Unknown argument: " + wrongArgument);
            System.err.println("ant [options] [target [target2 [target3] ...]]");
            parser.printUsage(System.err);
            System.err.println();
            throw new IOException();
        }
    }
}
