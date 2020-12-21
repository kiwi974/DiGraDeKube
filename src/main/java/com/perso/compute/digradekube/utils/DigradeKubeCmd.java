package com.perso.compute.digradekube.utils;

import org.apache.commons.cli.*;

public class DigradeKubeCmd {

    public static CommandLine readCmd(String[] args) {

        Options options = new Options();

        Option input = new Option("s", "self", true, "<name>:<port> of the current worker node.");
        input.setRequired(true);
        options.addOption(input);

        Option output = new Option("n", "neighbors", true, "Comma separated <name>:<port> of the neighbors worker nodes.");
        options.addOption(output);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("utility-name", options);

            System.exit(1);
        }

        return cmd;
    }

}
