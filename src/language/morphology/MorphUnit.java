/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package language.morphology;

import scala.collection.Seq;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @author wolf
 */
public class MorphUnit implements Serializable, Comparable<MorphUnit> {

    public static String NONE = "NONE";
    private String finalPos;
    private String primaryPos;
    private String secondaryPos;
    private String tagString;
    private String fullSurface;
    private String stem, lemma, token;
    private List<String[]> tags;
    private List<String> array;
    private boolean selected;
    private boolean updated = false;
    private double prob = 0.0;

    private static final List<String[]> replaces;

    static {
        replaces = new LinkedList<>();
        replaces.add(new String[]{"Adj^DB", "Adj"});
        replaces.add(new String[]{"Abil", "Able"});
        replaces.add(new String[]{"Adverb", "Adv"});
        replaces.add(new String[]{"Inst", "Ins"});
        replaces.add(new String[]{"Inf2", "Inf"});
        replaces.add(new String[]{"Ques", "QuesP"});
        replaces.add(new String[]{"Pers", "PersP"});
        replaces.add(new String[]{"Demons", "DemonsP"});
        replaces.add(new String[]{"Reflex", "ReflexP"});
        replaces.add(new String[]{"ReflexPP", "ReflexP"});
    }

    public MorphUnit(String token) {

        tags = new LinkedList<>();
        array = new LinkedList<>();
        tagString = "";


        this.token = token;
        this.finalPos = NONE;
        this.primaryPos = NONE;
        this.secondaryPos = NONE;
        this.stem = NONE;
        this.lemma = NONE;
        this.prob = 0.0;


    }

    public MorphUnit(String primaryPos, String stem) {
        tags = new LinkedList<>();
        array = new LinkedList<>();

        tagString = "";


        this.primaryPos = primaryPos;
        this.secondaryPos = NONE;
        this.stem = stem;
        this.lemma = stem;
        this.prob = 0.0;


        array.add(stem);
        array.add(primaryPos);

    }

    public MorphUnit(String primaryPos, String secondaryPos, String stem, String lemma) {
        tags = new LinkedList<>();
        array = new LinkedList<>();
        tagString = "";


        this.primaryPos = primaryPos;
        this.secondaryPos = secondaryPos;
        this.stem = stem;
        this.lemma = lemma;
        this.prob = 0.0;

        array.add(stem);
        array.add(primaryPos);
    }

    public void setPrimaryPos(String primaryPos) {

        this.primaryPos = primaryPos;
    }

    public void setSecondaryPos(String secondaryPos) {

        this.secondaryPos = secondaryPos;
    }


    public void setFinalPos(String finalPos) {
        this.finalPos = finalPos;
    }

    public void setStem(String stem) {
        this.stem = stem;
    }

    public String getFullSurface() {
        return fullSurface;
    }

    public void setFullSurface(String fullSurface) {
        this.fullSurface = fullSurface;
    }

    public void setFullSurfaceFromToken(String token) {
        String mstem = stem;
        int index = stem.indexOf("(");
        if (index != -1) {
            mstem = stem.substring(0, index);
        }

        if (mstem.equals(NONE) || primaryPos.equals(NONE) || mstem.length() > token.length()) {
            this.fullSurface = NONE;
        } else {

            this.fullSurface = token.substring(mstem.length());
        }
    }

    public void setLemma(String lemma) {
        this.lemma = lemma;
    }

    public void setProb(double prob) {
        this.prob = prob;
    }

    public String getPrimaryPos() {
        return primaryPos;
    }

    public String getSecondaryPos() {
        return secondaryPos;
    }

    public String getFinalPos() {
        return finalPos;
    }

    public String getStem() {
        return stem;
    }

    public String getLemma() {
        return lemma;
    }

    public double getProb() {
        return prob;
    }

    public String getSuffix(String token) {
        if (stem.length() >= token.length()) return NONE;
        else return token.substring(stem.length());
    }

    public boolean isPrimaryPos() {
        return !primaryPos.equals(NONE);
    }

    public boolean isSecondaryPos() {
        return !secondaryPos.equals(NONE);
    }

    public boolean isFinalPos() {
        return !finalPos.equals(NONE);
    }

    public List<String[]> getTags() {
        return tags;
    }

    public Seq<String> getTagsAsSeq(){
        List<String> tgs = new ArrayList<>();

        for(String[] tg:tags){
           tgs.add(tg[0]);
        }

        return scala.collection.JavaConversions.collectionAsScalaIterable(tgs).toSeq();
    }

    public boolean containsTag(String tag) {
        return array.contains(tag);
    }

    public boolean matchesTag(String tagRegex) {
        String tags = toString();
        return tags.matches(tagRegex);
    }

    public String buildFullTagString() {


        tagString = stem;

        for (String[] tag : tags) {

            if (!tag[0].equals(NONE)) {
                tagString += "+" + tag[0];
            }
            else if(tag[0].equals(NONE) && token.matches("\\p{Punct}+")){
                tagString = token + "+Punc";
            }
        }

        if(tagString.equals(NONE) && token.matches("\\p{Punct}+")){
            tagString = token + "+Punc";
        }

        return tagString;
    }

    public String buildTagString(boolean includeSecondaryPos) {
        tagString = "";
        if (!primaryPos.equals(NONE)) {
            tagString += "+" + primaryPos;
        }

        if (includeSecondaryPos && !secondaryPos.equals(NONE)) {
            tagString += "+" + secondaryPos;
        }

        for (String[] tag : tags) {

            if (!tag[0].equals(NONE)) {
                tagString += "+" + tag[0];
            }
        }

        return tagString;
    }

    public String[] getArray() {
        return array.toArray(new String[0]);
    }

    public String getTagString() {
        return tagString;
    }

    public String[] getTagArray() {

        return array.toArray(new String[0]);
    }
    public List<String> getTagList() {

        return array;
    }

    public boolean isUpdated() {
        return updated;
    }

    public void setUpdated(boolean updated) {
        this.updated = updated;
    }

    @Override
    public String toString() {
        if (tagString.isEmpty() || updated) return buildFullTagString();
        else return tagString;
    }

    @Override
    public int compareTo(MorphUnit o) {
        if(o.prob>prob) return -1;
        else if(o.prob<prob) return 1;
        else return 0;
    }

    public void addTag(String val, String tag) {
        for (String[] replacement : replaces) {
            tag = tag.replaceAll(replacement[0], replacement[1]);
        }
        tags.add(new String[]{tag, val});
        array.add(tag);
    }

    public boolean isNone() {
        return stem.equals(NONE);
    }

    public boolean isSelected() {
        return selected;
    }

    public void setSelected(boolean selected) {
        this.selected = selected;
    }


}
