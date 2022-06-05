package temp;

import java.util.Arrays;

public class GenericTopK {
    static String separator = "/S/";
    int k = 5;
    public String[] elementsArray = new String[this.k + 1];

    public GenericTopK() {
        this.elementsArray = new String[k + 1];
    }

    public GenericTopK(int k) {
        this.k = k;
        this.elementsArray = new String[k + 1];
    }

    @Override
    public String toString() {
        StringBuilder outString = new StringBuilder();

        for (int i = 0; i < this.elementsArray.length - 1; i++) {
            if (this.elementsArray[i] == null) {
                break;
            }
            String[] parts = this.elementsArray[i].split(separator);
            String current = (i + 1) + ". ID: " + parts[0] + ", Count: " + parts[1] + "\n";
            outString.append(current);
        }
        return outString.toString();
    }

    public boolean isNew(String instance) {
        // function to replace old instance with new one
        String key = instance.split(separator)[0];
        for (int i = 0; i < this.elementsArray.length - 1; i++) {
            if (this.elementsArray[i] == null) {
                return false;
            }
            if (this.elementsArray[i].split(separator)[0].equals(key)) {
                this.elementsArray[i] = instance;
                return true;
            }
        }
        return false;
    }
}