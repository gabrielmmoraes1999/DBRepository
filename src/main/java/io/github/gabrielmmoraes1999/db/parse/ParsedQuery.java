package io.github.gabrielmmoraes1999.db.parse;

import java.util.ArrayList;
import java.util.List;

public class ParsedQuery {

    public final List<List<Condition>> orGroups = new ArrayList<>();

    public final List<OrderBy> orderByList = new ArrayList<>();

}
