package broker.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Publication {

    private static final String simpleDateFormatString = "dd/MM/yyyy";
    private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat(simpleDateFormatString);

    private final String company;
    private final double value;
    private final double drop;
    private final double variation;
    @JsonFormat(shape=JsonFormat.Shape.STRING, pattern=simpleDateFormatString)
    private final Date date;

    public static Publication fromTuple(Tuple tuple) throws ParseException {
        String company = tuple.getStringByField("company");
        double value = tuple.getDoubleByField("value");
        double drop = tuple.getDoubleByField("drop");
        double variation = tuple.getDoubleByField("variation");
        String dateStr = tuple.getStringByField("date");
        Date date = simpleDateFormat.parse(dateStr);

        return new Publication(company, value, drop, variation, date);
    }

    public Publication(String company, double value, double drop, double variation, Date date) {
        this.company = company;
        this.value = value;
        this.drop = drop;
        this.variation = variation;
        this.date = date;
    }

    public String getCompany() {
        return this.company;
    }

    public double getValue() {
        return this.value;
    }

    public double getDrop() {
        return this.drop;
    }

    public double getVariation() {
        return this.variation;
    }

    public Date getDate() {
        return this.date;
    }

    public Values toValues() {
        return new Values(this.getCompany(), this.getValue(), this.getDrop(), this.getVariation(), this.getDate());
    }
}
