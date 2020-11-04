package com.opstty;

import com.opstty.job.*;
import org.apache.hadoop.util.ProgramDriver;

import java.nio.file.Path;
import java.nio.file.Paths;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();
        for(String ar:argv)
            System.out.println(ar);

        switch(argv[0]){
            case "wordcount":
                try {
                    programDriver.addClass("wordcount", WordCount.class,
                            "A map/reduce program that counts the words in the input files.");
                    exitCode = programDriver.run(argv);
                } catch (Throwable throwable) {
                    throwable.printStackTrace();
                }

            case "districtcount":
                try {
                    programDriver.addClass("districtcount", DistrictWithTreesCount.class,
                            "A map/reduce program that counts the districts that have trees.");
                    exitCode = programDriver.run(argv);
                } catch (Throwable throwable) {
                    throwable.printStackTrace();
                }
            case "existingspeciescount":
                try {
                    programDriver.addClass("existingspeciescount", ExistingSpeciesCount.class,
                            "A map/reduce program that counts the districts species.");
                    exitCode = programDriver.run(argv);
                } catch (Throwable throwable) {
                    throwable.printStackTrace();
                }
            case "treesbyspecies":
                try {
                    programDriver.addClass("treesbyspecies", TreesBySpeciesCount.class,
                            "A map/reduce program that counts the trees by species.");
                    exitCode = programDriver.run(argv);
                } catch (Throwable throwable) {
                    throwable.printStackTrace();
                }
            case "maximumheight":
                try {
                    programDriver.addClass("maximumheight", MaximumHeightPerSpecies.class,
                            "A map/reduce program that shows the maximum height by species.");
                    exitCode = programDriver.run(argv);
                } catch (Throwable throwable) {
                    throwable.printStackTrace();
                }
            case "treeheightsort":
                try {
                    programDriver.addClass("treeheightsort", TreeHeightSort.class,
                            "A map/reduce program that shows the maximum height by species.");
                    exitCode = programDriver.run(argv);
                } catch (Throwable throwable) {
                    throwable.printStackTrace();
                }
            case "oldesttree":
                try {
                    programDriver.addClass("oldesttree", OldestTree.class,
                            "A map/reduce program that shows the maximum height by species.");
                    exitCode = programDriver.run(argv);
                } catch (Throwable throwable) {
                    throwable.printStackTrace();
                }
            default:
                System.out.println("the program deos not exist. Application will quit.");
        }
        System.exit(exitCode);
    }
}
