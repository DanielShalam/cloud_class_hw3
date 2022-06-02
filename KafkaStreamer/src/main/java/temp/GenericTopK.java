package temp;

import java.util.Arrays;

public class GenericTopK {
    static String separator = "/S/";
    int k = 5;
    public String[] elementsArray = new String[this.k+1];

    public GenericTopK() {
        this.elementsArray = new String[k+1];
        Arrays.fill(this.elementsArray, null);
        System.out.println("Array not top k: ");
        System.out.println(Arrays.toString(this.elementsArray));

    }
    public GenericTopK(int k) {
//            this.elementsArray = new GenericCountHolder[k+1];
        this.k = k;
        this.elementsArray = new String[k+1];
        Arrays.fill(this.elementsArray, null);
    }

    @Override
    public String toString(){
        StringBuilder outString = new StringBuilder();

        for (int i = 0; i < this.elementsArray.length - 1; i++) {
            if (this.elementsArray[i] == null) {
                break;
            }
            String[] parts = this.elementsArray[i].split(separator);
            String current = (i+1) + ". ID: " + parts[0] + ", Count: " + parts[1] + "\n";
            outString.append(current);
            this.elementsArray[i] = null;
        }
        return outString.toString();
    }

}