package language.topicality;

import java.io.*;


import cc.mallet.types.*;
import cc.mallet.util.Randoms;

import gnu.trove.*;

public class HLDAInferencer implements Serializable {

    private HLDA.NCRPNode rootNode;
    int numWordsToDisplay = 25;

    //global variable
    int numLevels;
    int numDocuments;
    int numTypes;
    int totalNodes;
    public int counter = 0;

    double alpha; // smoothing on topic distributions
    double gamma; // "imaginary" customers at the next, as yet unused table
    double eta;   // smoothing on word distributions
    double etaSum;

    int[][] globalLevels; // indexed < doc, token >
    HLDA.NCRPNode[] golbalDocumentLeaves; // currently selected path (ie leaf node) through the NCRP tree

    //local variable

    int localLevels[];
    FeatureSequence fs;

    HLDA.NCRPNode leaveNode;


    Randoms random;


    public HLDAInferencer(HLDA hLDA) {
        this.setRootNode(hLDA.rootNode);
        this.numLevels = hLDA.numLevels;
        this.numDocuments = hLDA.numDocuments;
        this.numTypes = hLDA.numTypes;

        this.alpha = hLDA.alpha;
        this.gamma = hLDA.gamma;
        this.eta = hLDA.eta;
        this.etaSum = hLDA.etaSum;
        this.totalNodes = hLDA.totalNodes;

        this.globalLevels = hLDA.levels;
        this.golbalDocumentLeaves = hLDA.documentLeaves;
        this.random = hLDA.random;
    }

    public StringBuffer getSampledDistribution(Instance instance, int numIterations) {

        HLDA.NCRPNode node;
        fs = (FeatureSequence) instance.getData();
        int docLength = fs.size();

        localLevels = new int[docLength];
        HLDA.NCRPNode[] path = new HLDA.NCRPNode[numLevels];

        //initialize
        //1.generate path
        path[0] = getRootNode();
        for (int level = 1; level < numLevels; level++) {
            path[level] = path[level - 1].selectExisting();
        }
        leaveNode = path[numLevels - 1];

        //2. randomly put tokens to different levels
        for (int token = 0; token < docLength; token++) {
            int type = fs.getIndexAtPosition(token);

            //ignore words otside dctionary

            if (type < numTypes) {
                localLevels[token] = random.nextInt(numLevels);
                node = path[localLevels[token]];
                node.totalTokens++;
                node.typeCounts[type]++;
            }
        }

        //for each iteration
        for (int iteration = 1; iteration <= numIterations; iteration++) {
            //1.sample path
            samplePath();
            //2.sampe topics
            sampleTopics();
        }

        return printTopicDistribution(leaveNode, localLevels);
    }

    private void samplePath() {

        int level, token, type;
        HLDA.NCRPNode node;
        HLDA.NCRPNode[] path = new HLDA.NCRPNode[numLevels];
        //1.get current path
        node = leaveNode;
        for (level = numLevels - 1; level >= 0; level--) {
            path[level] = node;
            node = node.parent;
        }

        TObjectDoubleHashMap<HLDA.NCRPNode> nodeWeights = new TObjectDoubleHashMap<HLDA.NCRPNode>();

        // Calculate p(c_m | c_{-m})
        calculateNCRP(nodeWeights, getRootNode(), 0.0);

        TIntIntHashMap[] typeCounts = new TIntIntHashMap[numLevels];
        int[] docLevels;

        for (level = 0; level < numLevels; level++) {
            typeCounts[level] = new TIntIntHashMap();
        }
        docLevels = localLevels;

        // Save the counts of every word at each level, and remove
        //  counts from the current path
        for (token = 0; token < docLevels.length; token++) {
            level = docLevels[token];
            type = fs.getIndexAtPosition(token);

            //ignore words outside dictionary
            if (type < numTypes) {

                if (!typeCounts[level].containsKey(type)) {
                    typeCounts[level].put(type, 1);
                } else {
                    typeCounts[level].increment(type);
                }

                path[level].typeCounts[type]--;
                assert (path[level].typeCounts[type] >= 0);

                path[level].totalTokens--;
                assert (path[level].totalTokens >= 0);
            }

        }

        double[] newTopicWeights = new double[numLevels];
        for (level = 1; level < numLevels; level++) {  // Skip the root...
            int[] types = typeCounts[level].keys();
            int totalTokens = 0;

            for (int t : types) {
                for (int i = 0; i < typeCounts[level].get(t); i++) {
                    newTopicWeights[level] +=
                            Math.log((eta + i) / (etaSum + totalTokens));
                    totalTokens++;
                }
            }

            //if (iteration > 1) { System.out.println(newTopicWeights[level]); }
        }

        calculateWordLikelihood(nodeWeights, getRootNode(), 0.0, typeCounts, newTopicWeights, 0);

        HLDA.NCRPNode[] nodes = nodeWeights.keys(new HLDA.NCRPNode[]{});
        double[] weights = new double[nodes.length];
        double sum = 0.0;
        double max = Double.NEGATIVE_INFINITY;

        // To avoid underflow, we're using log weights and normalizing the node weights so that
        //  the largest weight is always 1.
        for (int i = 0; i < nodes.length; i++) {
            if (nodeWeights.get(nodes[i]) > max) {
                max = nodeWeights.get(nodes[i]);
            }
        }

        for (int i = 0; i < nodes.length; i++) {
            weights[i] = Math.exp(nodeWeights.get(nodes[i]) - max);

            sum += weights[i];
        }

        //if (iteration > 1) {System.out.println();}
        node = nodes[random.nextDiscrete(weights, sum)];

        // If we have picked an internal node, we need to
        //  add a new path.
        while (!node.isLeaf()) {
            node = node.selectExisting();
        }

        leaveNode = node;

        HLDA.NCRPNode node2 = node;
        for (level = numLevels - 1; level >= 0; level--) {
            int[] types = typeCounts[level].keys();

            for (int t : types) {
                if (t < numTypes) {
                    node2.typeCounts[t] += typeCounts[level].get(t);
                    node2.totalTokens += typeCounts[level].get(t);
                }
            }

            node2 = node2.parent;
        }

    }

    private void sampleTopics() {

        int seqLen = fs.getLength();
        int[] docLevels = localLevels;
        HLDA.NCRPNode[] path = new HLDA.NCRPNode[numLevels];
        HLDA.NCRPNode node;
        int[] levelCounts = new int[numLevels];
        int type, token, level;
        double sum;

        // Get the leaf
        node = leaveNode;
        for (level = numLevels - 1; level >= 0; level--) {
            path[level] = node;
            node = node.parent;
        }

        double[] levelWeights = new double[numLevels];

        // Initialize level counts
        for (token = 0; token < seqLen; token++) {
            levelCounts[docLevels[token]]++;
        }

        for (token = 0; token < seqLen; token++) {
            type = fs.getIndexAtPosition(token);

            //ignore words outside dictionary
            if (type >= numTypes) {
                continue;
            }

            levelCounts[docLevels[token]]--;
            node = path[docLevels[token]];
            node.typeCounts[type]--;
            node.totalTokens--;


            sum = 0.0;
            for (level = 0; level < numLevels; level++) {
                levelWeights[level] =
                        (alpha + levelCounts[level]) *
                                (eta + path[level].typeCounts[type]) /
                                (etaSum + path[level].totalTokens);
                sum += levelWeights[level];
            }
            level = random.nextDiscrete(levelWeights, sum);

            docLevels[token] = level;
            levelCounts[docLevels[token]]++;
            node = path[level];
            node.typeCounts[type]++;
            node.totalTokens++;
        }
        localLevels = docLevels;
    }


    public void calculateNCRP(TObjectDoubleHashMap<HLDA.NCRPNode> nodeWeights, HLDA.NCRPNode node, double weight) {

        for (HLDA.NCRPNode child : node.children) {
            calculateNCRP(nodeWeights, child,
                    weight + Math.log((double) child.customers / (node.customers + gamma)));
        }

        nodeWeights.put(node, weight + Math.log(gamma / (node.customers + gamma)));
    }

    public void calculateWordLikelihood(TObjectDoubleHashMap<HLDA.NCRPNode> nodeWeights,
                                        HLDA.NCRPNode node, double weight,
                                        TIntIntHashMap[] typeCounts, double[] newTopicWeights,
                                        int level) {

        // First calculate the likelihood of the words at this level, given this topic.
        double nodeWeight = 0.0;
        int[] types = typeCounts[level].keys();
        int totalTokens = 0;

        //if (iteration > 1) { System.out.println(level + " " + nodeWeight); }

        for (int type : types) {
            for (int i = 0; i < typeCounts[level].get(type); i++) {
                nodeWeight +=
                        Math.log((eta + node.typeCounts[type] + i) / (etaSum + node.totalTokens + totalTokens));
                totalTokens++;

            }
        }

        // Propagate that weight to the child nodes

        for (HLDA.NCRPNode child : node.children) {
            calculateWordLikelihood(nodeWeights, child, weight + nodeWeight,
                    typeCounts, newTopicWeights, level + 1);
        }

        // Finally, if this is an internal node, add the weight of
        //  a new path

        level++;
        while (level < numLevels) {
            nodeWeight += newTopicWeights[level];
            level++;
        }

        nodeWeights.adjustValue(node, nodeWeight);
    }

    public StringBuffer printTopicDistribution(HLDA.NCRPNode leave, int[] levels) {
        HLDA.NCRPNode node = leave;
        HLDA.NCRPNode[] path = new HLDA.NCRPNode[numLevels];
        for (int level = numLevels - 1; level >= 0; level--) {
            path[level] = node;
            node = node.parent;
        }

        double[] result = new double[totalNodes];
        StringBuffer out = new StringBuffer();

        int[] levelCounts = new int[numLevels];
        for (int i = 0; i < levels.length; i++) {
            levelCounts[levels[i]]++;
        }

        double sum = 0.0;
        for (int level = 0; level < numLevels; level++) {
            sum += (alpha + levelCounts[level]);
        }

        for (int level = 0; level < numLevels; level++) {
            result[path[level].nodeID] = alpha + levelCounts[level] / sum;
        }

        for (int i = 0; i < numLevels; i++) {

            out.append("nodeID:" + path[i].nodeID + ",");
            out.append((alpha + levelCounts[i]) / sum + ",");
        }

        return (out);
    }

    public StringBuffer printTopicDistribution(int doc) {
        return printTopicDistribution(golbalDocumentLeaves[doc], globalLevels[doc]);
    }

    public void getTopicDistribution(HLDA.NCRPNode leave, int[] levels) {

        HLDA.NCRPNode node = leave;
        HLDA.NCRPNode[] path = new HLDA.NCRPNode[numLevels];
        for (int level = numLevels - 1; level >= 0; level--) {
            path[level] = node;
            node = node.parent;
        }

        double[] result = new double[totalNodes];

        int[] levelCounts = new int[numLevels];
        for (int i = 0; i < levels.length; i++) {
            levelCounts[levels[i]]++;
        }

        double sum = 0.0;
        for (int level = 0; level < numLevels; level++) {
            sum += (alpha + levelCounts[level]);
        }

        for (int level = 0; level < numLevels; level++) {
            result[path[level].nodeID] = alpha + levelCounts[level] / sum;
        }

        for (int i = 0; i < numLevels; i++) {
            System.out.println("Level:" + i + "\tnodeID:" + path[i].nodeID + "\t" + (alpha + levelCounts[i]) / sum);
        }

    }

    public void getTopicDistribution(int doc) {

        getTopicDistribution(golbalDocumentLeaves[doc], globalLevels[doc]);
    }


    public void printNode(HLDA.NCRPNode node, int indent) {
        //reset nodes counter
        if (node.nodeID == 0) {
            counter = 0;
        }
        StringBuffer out = new StringBuffer();
        for (int i = 0; i < indent; i++) {
            out.append("  ");
        }

        out.append("ID:" + node.nodeID + ",Tokens:" + node.totalTokens + ",customers:" + node.customers + "/ ");
        out.append(node.getTopWords(numWordsToDisplay));
        System.out.println(out);
        counter++;

        for (HLDA.NCRPNode child : node.children) {
            printNode(child, indent + 1);
        }
    }

    public void printNodeTofile(HLDA.NCRPNode node, int indent, BufferedWriter writer) {

        //reset nodes counter
        if (node.nodeID == 0) {
            counter = 0;
        }
        StringBuffer out = new StringBuffer();

			/*
			for (int i=0; i<indent; i++) {
				out.append("  ");
			}
			
            out.append("layer:"+indent+",");
			out.append("ID:" + node.nodeID + ",Tokens:"+ node.totalTokens + ",customers:" + node.customers + ",");
			out.append(node.getTopWords(numWordsToDisplay));
			out.append("\n");
			*/

        out.append(indent + ",");
        out.append(node.nodeID + "," + node.totalTokens + "," + node.customers + ",");
        out.append(node.getTopWords(numWordsToDisplay));
        out.append("\n");


        try {
            writer.write(out.toString());
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        counter++;

        for (HLDA.NCRPNode child : node.children) {
            printNodeTofile(child, indent + 1, writer);
        }
    }

    public void printTrainData(InstanceList instances, BufferedWriter writer) {
        try {
            for (int i = 0; i < instances.size(); i++) {
                writer.write(instances.get(i).getName() + "," + printTopicDistribution(i).toString() + "\n");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void printTestData(InstanceList instances, int interations, BufferedWriter writer) {

        try {
            for (int i = 0; i < instances.size(); i++) {
                writer.write(instances.get(i).getName() + "," + getSampledDistribution(instances.get(i), 300).toString() + "\n");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public HLDA.NCRPNode getRootNode() {
        return rootNode;
    }

    public void setRootNode(HLDA.NCRPNode rootNode) {
        this.rootNode = rootNode;
    }
}

