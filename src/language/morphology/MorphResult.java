/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package language.morphology;


import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.io.Serializable;
import java.util.*;

/**
 * @author wolf
 */
public class MorphResult implements Serializable {

    private String token;
    private List<MorphUnit> resultList;
    private List<String[]> resultListArray;
    private List<String> stemList;
    private boolean isNone;
    private MorphUnit label;

    public MorphResult(String token) {
        this.resultList = new LinkedList<>();
        this.resultListArray = new LinkedList<>();
        this.stemList = new LinkedList<>();
        this.token = token;
        this.isNone = false;
    }

    public MorphResult(String token, String pos, String stem) {
        this.resultList = new LinkedList<>();
        this.resultListArray = new LinkedList<>();
        this.stemList = new LinkedList<>();
        this.token = token;
        this.isNone = false;

        addMorphUnit(new MorphUnit(pos, stem));
    }

    public void addMorphUnit(MorphUnit morphUnit) {

        morphUnit.setFullSurfaceFromToken(token);

        resultList.add(morphUnit);
        resultListArray.add(morphUnit.getArray());
        stemList.add(morphUnit.getStem());
        isNone = morphUnit.isNone();
    }

    public MorphUnit getLabel() {
        return label;
    }

    public void setLabel(MorphUnit label) {
        this.label = label;
    }

    public void shuffle() {
        Collections.shuffle(resultList);
    }

    public List<MorphUnit> getResultList() {
        return resultList;
    }

    public List<String> getTagStringList() {
        List<String> tagStringList = new ArrayList<>();
        for (MorphUnit unit : resultList) {
            tagStringList.add(unit.toString());
        }
        return tagStringList;
    }

    public Seq<MorphUnit> getResultSeq() {
        return JavaConversions.asScalaIterable(resultList).toSeq();
    }

    public Seq<String> getTagStringSeq() {
        List<String> tagStrings = getTagStringList();
        return JavaConversions.asScalaIterable(tagStrings).toVector();
    }

    public MorphUnit getFirstResult() {
        if (resultList.isEmpty()) return new MorphUnit(MorphUnit.NONE, MorphUnit.NONE);
        else return resultList.get(0);
    }


    public void clearUnLikely() {
        Collections.sort(resultList);
        MorphUnit first = getFirstResult();
        resultList.clear();
        resultList.add(first);
    }


    public void setResultList(List<MorphUnit> resultList) {
        this.resultList = resultList;
    }

    public List<String> getStemList() {
        return stemList;
    }

    public boolean getNone() {
        return isNone;
    }

    @Override
    public String toString() {
        String val = "";

        val += "[";
        for (MorphUnit morphUnit : resultList) {
            val += "[" + morphUnit.buildFullTagString() + "]";
        }

        val += "]";
        return val;
    }

    public void toStringList(List<String> result) {
        for (MorphUnit morphUnit : resultList) {
            result.add(morphUnit.buildFullTagString());
        }
    }

    public String toLineString() {
        String[] resultArray = toStringArray();
        String line = token;
        for (String value : resultArray) {
            line += " " + value;
        }

        return line;
    }

    public String[] toStringArray() {
        String[] array = new String[resultList.size()];

        for (int i = 0; i < resultList.size(); i++) {
            MorphUnit morphUnit = resultList.get(i);
            array[i] = morphUnit.buildFullTagString();
        }

        return array;
    }

    public String[] toStringArray(boolean includeSecondaryPos) {
        String[] array = new String[resultList.size()];


        for (int i = 0; i < resultList.size(); i++) {
            MorphUnit morphUnit = resultList.get(i);
            array[i] = morphUnit.buildTagString(includeSecondaryPos);

        }

        return array;
    }

    public String[] toPosTagArray() {
        List<String> tags = new LinkedList<>();

        for (int i = 0; i < resultList.size(); i++) {
            MorphUnit morphUnit = resultList.get(i);
            tags.add(morphUnit.getPrimaryPos());

        }

        return tags.toArray(new String[0]);
    }


    public String[] toSurfaceForms() {
        String[] surfaces = new String[resultList.size()];
        for (int i = 0; i < resultList.size(); i++) {
            surfaces[i] = resultList.get(i).getFullSurface();


        }

        return surfaces;
    }

    public String[][] toTagArray() {
        String[][] tags = new String[resultList.size()][];
        for (int i = 0; i < tags.length; i++) {
            MorphUnit morphUnit = resultList.get(i);
            String[] tagArray = morphUnit.getTagArray();
            tags[i] = tagArray;
        }

        return tags;
    }

    public String[] toStemArray() {
        return stemList.toArray(new String[0]);
    }

    public String[] toUniqueTagArray() {
        Set<String> tags = new TreeSet<>();

        for (int i = 0; i < resultList.size(); i++) {
            MorphUnit morphUnit = resultList.get(i);
            String[] tagArray = morphUnit.getTagArray();
            String pos = morphUnit.getPrimaryPos();
            if (!pos.isEmpty()) {
                tags.add(pos);
            }
            tags.addAll(Arrays.asList(tagArray));
        }

        return tags.toArray(new String[0]);
    }

    private List<String> differenceLabelTags() {
        List<String> targetTags = new ArrayList<>(label.getTagList());

        if (!targetTags.isEmpty() && resultList.size()>1) {
            targetTags = targetTags.subList(1, targetTags.size());

            List<String> currentTags = new ArrayList<>(targetTags);
            for (MorphUnit aunit : resultList) {

                if (!aunit.equals(label)) {
                    for (String currentTag : currentTags) {
                        if (aunit.containsTag(currentTag)) {
                            targetTags.remove(currentTag);
                        }
                    }
                }
            }
            return targetTags;
        } else {
            return new ArrayList<>();
        }
    }

    public List<String> minimumDifferenceSet() {
        if (label == null) {
            Set<String> tagSet = new HashSet<>();

            for (MorphUnit morphUnit : resultList) {
                setLabel(morphUnit);
                List<String> differenceList = differenceLabelTags();
                tagSet.addAll(differenceList);
            }

            return new ArrayList<String>(tagSet);
        } else {
            return differenceLabelTags();
        }
    }

    public Seq<String> minimumDifferenceSetAsSeq() {
        return JavaConversions.asScalaIterable(minimumDifferenceSet()).toVector();
    }

    public List<String[]> getListArray() {
        return resultListArray;
    }

    public MorphUnit getBestScore() {
        if (resultList.size() > 0) {
            return resultList.get(0);
        } else {
            return null;
        }
    }

    public int size() {
        return resultList.size();
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public boolean isNone() {
        if (size() == 1) {
            return resultList.get(0).isNone();
        } else {
            return false;
        }
    }

    public void clear() {
        resultList.clear();
    }

    public String getToken() {
        return token;
    }
}
