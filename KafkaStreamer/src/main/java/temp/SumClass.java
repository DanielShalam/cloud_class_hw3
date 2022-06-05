package temp;

import java.util.LinkedList;

public class SumClass {
    static String separator = "/S/";
    public LinkedList<String> keysArray;
    public Integer sum = 0;

    public SumClass() {
        this.keysArray = new LinkedList<>();
        this.sum = 0;
    }

    public SumClass add(String value){
        String[] parts = value.split(separator);

        for (int i = 0; i < this.keysArray.size(); i++) {
            if (this.keysArray.get(i).split(separator)[0].equals(parts[0])) {
                int val = Integer.parseInt(this.keysArray.get(i).split(separator)[1]);
                this.sum -= val;
                this.sum += Integer.parseInt(parts[1]);
                this.keysArray.set(i, value);
                return this;
            }
        }

        this.keysArray.add(value);
        this.sum += Integer.parseInt(parts[1]);
        return this;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        String sumString = this.sum.toString();
        for (String string: this.keysArray){
            String[] parts = string.split(separator);
            String toBuild = "Key: " + parts[0] + " Part: " + parts[1];
            builder.append("\n" + toBuild + "/" + sumString + "\n");
        }
        return builder.toString();
    }

}
