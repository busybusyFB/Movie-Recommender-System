public class Driver {
    public static void main(String[] args) throws Exception {
        if (args.length != 6) {
            return;
        }
        String rawInput = args[0];
        String userMovieListOutputDir = args[1];
        String coOccurrenceMatrixDir = args[2];
        String normalizeDir = args[3];
        String multiplicationDir = args[4];
        String sumDir = args[5];

        DataDividerByUser.main(new String[]{rawInput, userMovieListOutputDir});
        CooccuranceMatrixBuilder.main(new String[]{userMovieListOutputDir, coOccurrenceMatrixDir});
        Normalizer.main(new String[]{coOccurrenceMatrixDir, normalizeDir});
        Multiplication.main(new String[]{normalizeDir, rawInput, multiplicationDir});
        Sum.main(new String[]{multiplicationDir, sumDir});

    }
}
