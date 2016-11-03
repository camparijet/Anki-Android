/****************************************************************************************
 * Copyright (c) 2012 Norbert Nagold <norbert.nagold@gmail.com>                         *
 * Copyright (c) 2012 Kostas Spyropoulos <inigo.aldana@gmail.com>                       *
 * Copyright (c) 2014 Houssam Salem <houssam.salem.au@gmail.com>                        *
 *                                                                                      *
 * This program is free software; you can redistribute it and/or modify it under        *
 * the terms of the GNU General Public License as published by the Free Software        *
 * Foundation; either version 3 of the License, or (at your option) any later           *
 * version.                                                                             *
 *                                                                                      *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY      *
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A      *
 * PARTICULAR PURPOSE. See the GNU General Public License for more details.             *
 *                                                                                      *
 * You should have received a copy of the GNU General Public License along with         *
 * this program.  If not, see <http://www.gnu.org/licenses/>.                           *
 ****************************************************************************************/

package com.ichi2.libanki;

import android.database.Cursor;
import android.database.SQLException;
import android.text.TextUtils;

import android.util.Pair;

import com.ichi2.async.DeckTask;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import timber.log.Timber;

public class Finder {

    private static final Pattern fPropPattern = Pattern.compile("(^.+?)(<=|>=|!=|=|<|>)(.+?$)");
    private static final Pattern fNidsPattern = Pattern.compile("[^0-9,]");
    private static final Pattern fMidPattern = Pattern.compile("[^0-9]");

    private Collection mCol;


    public Finder(Collection col) {
        mCol = col;
    }


    /*
     * NOTE: The python version of findCards can accept a boolean, a string, or no value for the _order parameter. The
     * type of _order also determines which _order() method is used. To maintain type safety, we expose the three valid
     * options here and safely type-cast accordingly at run-time.
     */

    /** Return a list of card ids for QUERY */
    public List<Long> findCards(String query) {
        return findCards(query, false);
    }


    public List<Long> findCards(String query, String _order) {
        return _findCards(query, _order);
    }


    public List<Long> findCards(String query, boolean _order) {
        return _findCards(query, _order);
    }


    private List<Long> _findCards(String query, Object _order) {
        String[] tokens = _tokenize(query);
        Pair<String, String[]> res1 = _where(tokens);
        String preds = res1.first;
        String[] args = res1.second;
        List<Long> res = new ArrayList<>();
        if (preds == null) {
            return res;
        }
        Pair<String, Boolean> res2 = _order instanceof Boolean ? _order((Boolean) _order) : _order((String) _order);
        String order = res2.first;
        boolean rev = res2.second;
        String sql = _query(preds, order);
        Cursor cur = null;
        try {
            cur = mCol.getDb().getDatabase().rawQuery(sql, args);
            while (cur.moveToNext()) {
                res.add(cur.getLong(0));
            }
        } catch (SQLException e) {
            // invalid grouping
            return new ArrayList<>();
        } finally {
            if (cur != null) {
                cur.close();
            }
        }
        if (rev) {
            Collections.reverse(res);
        }
        return res;
    }


    public List<Long> findNotes(String query) {
        String[] tokens = _tokenize(query);
        Pair<String, String[]> res1 = _where(tokens);
        String preds = res1.first;
        String[] args = res1.second;
        List<Long> res = new ArrayList<>();
        if (preds == null) {
            return res;
        }
        if (preds.equals("")) {
            preds = "1";
        } else {
            preds = "(" + preds + ")";
        }
        String sql = "select distinct(n.id) from cards c, notes n where c.nid=n.id and " + preds;
        Cursor cur = null;
        try {
            cur = mCol.getDb().getDatabase().rawQuery(sql, args);
            while (cur.moveToNext()) {
                res.add(cur.getLong(0));
            }
        } catch (SQLException e) {
            // invalid grouping
            return new ArrayList<>();
        } finally {
            if (cur != null) {
                cur.close();
            }
        }
        return res;
    }


    /**
     * Tokenizing
     * ***********************************************************
     */

    public String[] _tokenize(String query) {
        char inQuote = 0;
        List<String> tokens = new ArrayList<>();
        String token = "";
        for (int i = 0; i < query.length(); ++i) {
            // quoted text
            char c = query.charAt(i);
            if (c == '\'' || c == '"') {
                if (inQuote != 0) {
                    if (c == inQuote) {
                        inQuote = 0;
                    } else {
                        token += c;
                    }
                } else if (token.length() != 0) {
                    // quotes are allowed to start directly after a :
                    if (token.endsWith(":")) {
                        inQuote = c;
                    } else {
                        token += c;
                    }
                } else {
                    inQuote = c;
                }
                // separator
            } else if (c == ' ') {
                if (inQuote != 0) {
                    token += c;
                } else if (token.length() != 0) {
                    // space marks token finished
                    tokens.add(token);
                    token = "";
                }
                // nesting
            } else if (c == '(' || c == ')') {
                if (inQuote != 0) {
                    token += c;
                } else {
                    if (c == ')' && token.length() != 0) {
                        tokens.add(token);
                        token = "";
                    }
                    tokens.add(String.valueOf(c));
                }
                // negation
            } else if (c == '-') {
                if (token.length() != 0) {
                    token += c;
                } else if (tokens.size() == 0 || !tokens.get(tokens.size() - 1).equals("-")) {
                    tokens.add("-");
                }
                // normal character
            } else {
                token += c;
            }
        }
        // if we finished in a token, add it
        if (token.length() != 0) {
            tokens.add(token);
        }
        return tokens.toArray(new String[tokens.size()]);
    }


    /**
     * Query building
     * ***********************************************************
     */

    /**
     * LibAnki creates a dictionary and operates on it with an inner function inside _where().
     * AnkiDroid combines the two in this class instead.
     */
    public class SearchState {
        public boolean isnot;
        public boolean isor;
        public boolean join;
        public String q = "";
        public boolean bad;

        public void add(String txt) {
            add(txt, true);
        }

        public void add(String txt, boolean wrap) {
            // failed command?
            if (TextUtils.isEmpty(txt)) {
                // if it was to be negated then we can just ignore it
                if (isnot) {
                    isnot = false;
                    return;
                } else {
                    bad = true;
                    return;
                }
            } else if (txt.equals("skip")) {
                return;
            }
            // do we need a conjunction?
            if (join) {
                if (isor) {
                    q += " or ";
                    isor = false;
                } else {
                    q += " and ";
                }
            }
            if (isnot) {
                q += " not ";
                isnot = false;
            }
            if (wrap) {
                txt = "(" + txt + ")";
            }
            q += txt;
            join = true;
        }
    }


    public Pair<String, String[]> _where(String[] tokens) {
        Timber.d("start internal _where");// to measure time @TODO please remove it if this request is merged
        // state and query
        SearchState s = new SearchState();
        ArrayList<Pair<String, String>> field_cmds = new ArrayList<>();
        List<String> args = new ArrayList<>();
        for (String token : tokens) {
            if (s.bad) {
                return new Pair<>(null, null);
            }
            // special tokens
            if (token.equals("-")) {
                s.isnot = true;
            } else if (token.equalsIgnoreCase("or")) {
                s.isor = true;
            } else if (token.equals("(")) {
                s.add(token, false);
                s.join = false;
            } else if (token.equals(")")) {
                s.q += ")";
                // commands
            } else if (token.contains(":")) {
                String[] spl = token.split(":", 2);
                String cmd = spl[0].toLowerCase(Locale.US);
                String val = spl[1];
                
                if (cmd.equals("added")) {
                    s.add(_findAdded(val));
                } else if (cmd.equals("card")) {
                    s.add(_findTemplate(val));
                } else if (cmd.equals("deck")) {
                    s.add(_findDeck(val));
                } else if (cmd.equals("mid")) {
                    s.add(_findMid(val));
                } else if (cmd.equals("nid")) {
                    s.add(_findNids(val));
                } else if (cmd.equals("cid")) {
                    s.add(_findCids(val));
                } else if (cmd.equals("note")) {
                    s.add(_findModel(val));
                } else if (cmd.equals("prop")) {
                    s.add(_findProp(val));
                } else if (cmd.equals("rated")) {
                    s.add(_findRated(val));
                } else if (cmd.equals("tag")) {
                    s.add(_findTag(val, args));
                } else if (cmd.equals("dupe")) {
                    s.add(_findDupes(val));
                } else if (cmd.equals("is")) {
                    s.add(_findCardState(val));
                } else {
                    field_cmds.add(new Pair<>(cmd, val));
                    //s.add(_findField(cmd, val));
                }
            // normal text search
            } else {
                s.add(_findText(token, args));
            }
        }
        if (!field_cmds.isEmpty()) {
            s.add(_findFields(field_cmds));
        }

        if (s.bad) {
            return new Pair<>(null, null);
        }
        Timber.d("end internal _where");// to measure time @TODO please remove it if this request is merged
        return new Pair<>(s.q, args.toArray(new String[args.size()]));
    }


    private String _query(String preds, String order) {
        // can we skip the note table?
        String sql;
        if (!preds.contains("n.") && !order.contains("n.")) {
            sql = "select c.id from cards c where ";
        } else {
            sql = "select c.id from cards c, notes n where c.nid=n.id and ";
        }
        // combine with preds
        if (!TextUtils.isEmpty(preds)) {
            sql += "(" + preds + ")";
        } else {
            sql += "1";
        }
        // order
        if (!TextUtils.isEmpty(order)) {
            sql += " " + order;
        }
        return sql;
    }


    /**
     * Ordering
     * ***********************************************************
     */

    /*
     * NOTE: In the python code, _order() follows a code path based on:
     * - Empty order string (no order)
     * - order = False (no order)
     * - Non-empty order string (custom order)
     * - order = True (built-in order)
     * The python code combines all code paths in one function. In Java, we must overload the method
     * in order to consume either a String (no order, custom order) or a Boolean (no order, built-in order).
     */
    
    private Pair<String, Boolean> _order(String order) {
        if (TextUtils.isEmpty(order)) {
            return _order(false);
        } else {
            // custom order string provided
            return new Pair<>(" order by " + order, false);
        }
    }
    
    private Pair<String, Boolean> _order(Boolean order) {
        if (!order) {
            return new Pair<>("", false);
        }
        try {
            // use deck default
            String type = mCol.getConf().getString("sortType");
            String sort = null;
            if (type.startsWith("note")) {
                if (type.startsWith("noteCrt")) {
                    sort = "n.id, c.ord";
                } else if (type.startsWith("noteMod")) {
                    sort = "n.mod, c.ord";
                } else if (type.startsWith("noteFld")) {
                    sort = "n.sfld COLLATE NOCASE, c.ord";
                }
            } else if (type.startsWith("card")) {
                if (type.startsWith("cardMod")) {
                    sort = "c.mod";
                } else if (type.startsWith("cardReps")) {
                    sort = "c.reps";
                } else if (type.startsWith("cardDue")) {
                    sort = "c.type, c.due";
                } else if (type.startsWith("cardEase")) {
                    sort = "c.factor";
                } else if (type.startsWith("cardLapses")) {
                    sort = "c.lapses";
                } else if (type.startsWith("cardIvl")) {
                    sort = "c.ivl";
                }
            }
            if (sort == null) {
            	// deck has invalid sort order; revert to noteCrt
            	sort = "n.id, c.ord";
            }
            boolean sortBackwards = mCol.getConf().getBoolean("sortBackwards");
            return new Pair<>(" ORDER BY " + sort, sortBackwards);
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * Commands
     * ***********************************************************
     */

    private String _findTag(String val, List<String> args) {
        if (val.equals("none")) {
            return "n.tags = \"\"";
        }
        val = val.replace("*", "%");
        if (!val.startsWith("%")) {
            val = "% " + val;
        }
        if (!val.endsWith("%")) {
            val += " %";
        }
        args.add(val);
        return "n.tags like ?";
    }


    private String _findCardState(String val) {
        int n;
        if (val.equals("review") || val.equals("new") || val.equals("learn")) {
            if (val.equals("review")) {
                n = 2;
            } else if (val.equals("new")) {
                n = 0;
            } else {
                return "queue IN (1, 3)";
            }
            return "type = " + n;
        } else if (val.equals("suspended")) {
            return "c.queue = -1";
        } else if (val.equals("buried")) {
            return "c.queue = -2";
        } else if (val.equals("due")) {
            return "(c.queue in (2,3) and c.due <= " + mCol.getSched().getToday() +
                    ") or (c.queue = 1 and c.due <= " + mCol.getSched().getDayCutoff() + ")";
        } else {
            return null;
        }
    }


    private String _findRated(String val) {
        // days(:optional_ease)
        String[] r = val.split(":");
        int days;
        try {
            days = Integer.parseInt(r[0]);
        } catch (NumberFormatException e) {
            return null;
        }
        days = Math.min(days, 31);
        // ease
        String ease = "";
        if (r.length > 1) {
            if (!Arrays.asList("1", "2", "3", "4").contains(r[1])) {
                return null;
            }
            ease = "and ease=" + r[1];
        }
        long cutoff = (mCol.getSched().getDayCutoff() - 86400 * days) * 1000;
        return "c.id in (select cid from revlog where id>" + cutoff + " " + ease + ")";
    }


    private String _findAdded(String val) {
        int days;
        try {
            days = Integer.parseInt(val);
        } catch (NumberFormatException e) {
            return null;
        }
        long cutoff = (mCol.getSched().getDayCutoff() - 86400 * days) * 1000;
        return "c.id > " + cutoff;
    }


    private String _findProp(String _val) {
        // extract
        Matcher m = fPropPattern.matcher(_val);
        if (!m.matches()) {
            return null;
        }
        String prop = m.group(1).toLowerCase(Locale.US);
        String cmp = m.group(2);
        String sval = m.group(3);
        int val;
        // is val valid?
        try {
            if (prop.equals("ease")) {
                // LibAnki does this below, but we do it here to avoid keeping a separate float value.
                val = (int)(Double.parseDouble(sval) * 1000);
            } else {
                val = Integer.parseInt(sval);
            }
        } catch (NumberFormatException e) {
            return null;
        }
        // is prop valid?
        if (!Arrays.asList("due", "ivl", "reps", "lapses", "ease").contains(prop)) {
            return null;
        }
        // query
        String q = "";
        if (prop.equals("due")) {
            val += mCol.getSched().getToday();
            // only valid for review/daily learning
            q = "(c.queue in (2,3)) and ";
        } else if (prop.equals("ease")) {
            prop = "factor";
            // already done: val = int(val*1000)
        }
        q += "(" + prop + " " + cmp + " " + val + ")";
        return q;
    }


    private String _findText(String val, List<String> args) {
        val = val.replace("*", "%");
        args.add("%" + val + "%");
        args.add("%" + val + "%");
        return "(n.sfld like ? escape '\\' or n.flds like ? escape '\\')";
    }


    private String _findNids(String val) {
        if (fNidsPattern.matcher(val).find()) {
            return null;
        }
        return "n.id in (" + val + ")";
    }


    private String _findCids(String val) {
        if (fNidsPattern.matcher(val).find()) {
            return null;
        }
        return "c.id in (" + val + ")";
    }


    private String _findMid(String val) {
        if (fMidPattern.matcher(val).find()) {
            return null;
        }
        return "n.mid = " + val;
    }


    private String _findModel(String val) {
        LinkedList<Long> ids = new LinkedList<>();
        try {
            for (JSONObject m : mCol.getModels().all()) {
                if (m.getString("name").equalsIgnoreCase(val)) {
                    ids.add(m.getLong("id"));
                }
            }
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
        return "n.mid in " + Utils.ids2str(ids);
    }


    private List<Long> dids(Long did) {
        if (did == null) {
            return null;
        }
        TreeMap<String, Long> children = mCol.getDecks().children(did);
        List<Long> res = new ArrayList<>();
        res.add(did);
        res.addAll(children.values());
        return res;
    }


    public String _findDeck(String val) {
        // if searching for all decks, skip
        if (val.equals("*")) {
            return "skip";
            // deck types
        } else if (val.equals("filtered")) {
            return "c.odid";
        }
        List<Long> ids = null;
        // current deck?
        try {
            if (val.equalsIgnoreCase("current")) {
                ids = dids(mCol.getDecks().current().getLong("id"));
            } else if (!val.contains("*")) {
                // single deck
                ids = dids(mCol.getDecks().id(val, false));
            } else {
                // wildcard
                ids = new ArrayList<>();
                val = val.replace("*", ".*");
                val = val.replace("+", "\\+");
                for (JSONObject d : mCol.getDecks().all()) {
                    if (d.getString("name").matches("(?i)" + val)) {
                        for (long id : dids(d.getLong("id"))) {
                            if (!ids.contains(id)) {
                                ids.add(id);
                            }
                        }
                    }
                }
            }
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
        if (ids == null || ids.size() == 0) {
            return null;
        }
        String sids = Utils.ids2str(ids);
        return "c.did in " + sids + " or c.odid in " + sids;
    }


    private String _findTemplate(String val) {
        // were we given an ordinal number?
        Integer num = null;
        try {
            num = Integer.parseInt(val) - 1;
        } catch (NumberFormatException e) {
            num = null;
        }
        if (num != null) {
            return "c.ord = " + num;
        }
        // search for template names
        List<String> lims = new ArrayList<>();
        try {
            for (JSONObject m : mCol.getModels().all()) {
                JSONArray tmpls = m.getJSONArray("tmpls");
                for (int ti = 0; ti < tmpls.length(); ++ti) {
                    JSONObject t = tmpls.getJSONObject(ti);
                    if (t.getString("name").equalsIgnoreCase(val)) {
                        if (m.getInt("type") == Consts.MODEL_CLOZE) {
                            // if the user has asked for a cloze card, we want
                            // to give all ordinals, so we just limit to the
                            // model instead
                            lims.add("(n.mid = " + m.getLong("id") + ")");
                        } else {
                            lims.add("(n.mid = " + m.getLong("id") + " and c.ord = " +
                                    t.getInt("ord") + ")");
                        }
                    }
                }
            }
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
        return TextUtils.join(" or ", lims.toArray(new String[lims.size()]));
    }

    /**
     * utility class to get combination of fields
     */
    private ArrayList<String[]> _mapCombination(Map<String, ArrayList<String>> _fields) {
        ArrayList<String[]> res = new ArrayList<>();
        int _size = 1;
        int _nfields = _fields.size();
        for (ArrayList<String> e : _fields.values()) {
            _size *= e.size();
        }
        for (int i_size = 0; i_size < _size; i_size++) {
            res.add(new String[_nfields]);
        }

        String[] _keys = _fields.keySet().toArray(new String[_fields.keySet().size()]); // should be the same....

        int _repeat_size = _size;
        for (int i_field = 0; i_field < _nfields; i_field++) {
            int n_values = (_fields.get(_keys[i_field])).size();
            // for each field
            for (int i_value = 0, l = n_values; i_value < l; i_value++) {
                //for each value
                for (int i_repeat = 0, n_repeat = _size / _repeat_size; i_repeat < n_repeat; i_repeat++) {
                    for (int i_chunk = 0, _chunk_size = (_repeat_size / n_values); i_chunk < _chunk_size; i_chunk++) {

                        res.get(i_chunk + _chunk_size * i_value + i_repeat * _repeat_size)[i_field] = _fields.get(_keys[i_field]).get(i_value);
                    }
                }
            }
            _repeat_size /= n_values; // total times
        }
        return res;
    }

    /**
     * To solve the performance problem with a large Deck that can happen where every "field" parts in the user's Anki query raises one SQL to check those field.
     * This method will convert all "field" parts in the user's query into one SQL base on models.
     * It also optimize SQL by padding with "delimiter" value in field column in Database
     * @param field_cmds "field" part in the user's query
     * @return String to limit note id
     */
    private String _findFields(ArrayList<Pair<String, String>> field_cmds) {
        // @TODO order of fields
        Timber.d("start _findFields");

        Map<Long, int[]> mods = new HashMap<>(); // model-card
        // Map<Long, int[]> modflds = new HashMap<>(); // model-query-fields
        // key field name
        // value possible values in the field
        Map<String, ArrayList<String>> _fields = new LinkedHashMap<>();
        // @TODO validate if a field exists or not
        // value first if the field doesn't exist among all the model, it will be false ( can be used for UX ...?)

        // store field-value map
        for (int fci = 0, l = field_cmds.size(); fci < l; fci++) {
            Pair<String, String> f_cmd = field_cmds.get(fci);
        /*
         * We need two expressions to query the cards: One that will use JAVA REGEX syntax and another
         * that should use SQLITE LIKE clause syntax.
         */
            if (!_fields.containsKey(f_cmd.first)) {
                _fields.put(f_cmd.first, new ArrayList<String>());
            }
            _fields.get(f_cmd.first).add(
                    f_cmd.second.replace("%", "\\%") // For SQLITE, we escape all % signs
                            .replace("*", "%") // And then convert the * into non-escaped % signs
            );
        }

        // field keys array
        String[] _field_keys = _fields.keySet().toArray(new String[_fields.keySet().size()]);

        // generate model-field map
        for (int fci = 0, l = _field_keys.length; fci < l; fci++) {
            try {
                for (JSONObject m : mCol.getModels().all()) {
                    JSONArray flds = m.getJSONArray("flds");
                    for (int fi = 0; fi < flds.length(); ++fi) {
                        JSONObject f = flds.getJSONObject(fi);
                        if (f.getString("name").equalsIgnoreCase(_field_keys[fci])) {
                            if (!(mods.containsKey(m.getLong("id")))) {

                                int[] m_mods = new int[flds.length()];
                                for (int m_mfi = 0; m_mfi < m_mods.length; m_mfi++) {
                                    m_mods[m_mfi] = -1;
                                }
                                mods.put(m.getLong("id"), m_mods);

//                                int[] m_mflds = new int[l];
//                                for (int m_fi = 0; m_fi < m_mflds.length; m_fi++) {
//                                    m_mflds[m_fi] = -1;
//                                }
//                                modflds.put(m.getLong("id"), m_mflds);
                            }
                            mods.get(m.getLong("id"))[f.getInt("ord")] = fci; // card -> field index
//                            modflds.get(m.getLong("id"))[fci] = f.getInt("ord"); // field -> card index
                        }
                    }
                }
            } catch (JSONException e) {
                throw new RuntimeException(e);
            }
        }
        Timber.d("end _findFields model");
        if (mods.isEmpty()) {
            // nothing has that field
            return null;
        }

        // generate field-combination
        ArrayList<String[]> field_comb = _mapCombination(_fields);

        // generate model-field combination

        // generate query
        String rawSql = "select id, mid from notes where ";
        final String F_DEL = "||CHAR(31)||";
        final String SQL_DEL = " or ";
        ArrayList<String> sqlVals = new ArrayList<String>();
        for (Map.Entry<Long, int[]> e : mods.entrySet()) {
            for (int i_flds_cmb = 0, n_flds_cmb = field_comb.size(); i_flds_cmb < n_flds_cmb; i_flds_cmb++) {
                rawSql += "( mid = ? and flds like ";
                int[] m_flds = e.getValue();
                sqlVals.add(e.getKey().toString());
                // the query is generated based on model->flds order
                for (int m_fld_i = 0; m_fld_i < m_flds.length; m_fld_i++) {
                    if (m_flds[m_fld_i] >= 0) {
                        sqlVals.add(field_comb.get(i_flds_cmb)[m_flds[m_fld_i]]); // delimiter is 13
                    } else {
                        sqlVals.add("%");
                    }
                    rawSql += "?" + F_DEL;
                }
                rawSql = rawSql.substring(0, rawSql.length() - F_DEL.length());
                rawSql += " escape '\\' ) or ";
            }
        }
        rawSql = rawSql.substring(0, rawSql.length() - SQL_DEL.length()); // remove last ' or ' (length: 4)

        LinkedList<Long> nids = new LinkedList<>();
        Cursor cur = null;
        Timber.d("start _findFields sqlquery");
        try {
            /*
             * Here we use the sqlVal expression, that is required for LIKE syntax in sqllite.
             * There is no problem with special characters, because only % and _ are special
             * characters in this syntax.
             */
            String[] sqlVal = new String[sqlVals.size()];
            sqlVal = sqlVals.toArray(sqlVal);
            cur = mCol.getDb().getDatabase().rawQuery(rawSql, sqlVal);

            while (cur.moveToNext()) {
                nids.add(cur.getLong(0));
            }
        } finally {
            if (cur != null) {
                cur.close();
            }
        }
        Timber.d("end _findFields sqlquery");
        if (nids.isEmpty()) {
            return "0";
        }
        return "n.id in " + Utils.ids2str(nids);
    }

    private String _findField(String field, String val) {
        Timber.d("start _findField");// to measure time @TODO please remove it if this request is merged
        /*
         * We need two expressions to query the cards: One that will use JAVA REGEX syntax and another
         * that should use SQLITE LIKE clause syntax.
         */
        String sqlVal = val
                .replace("%","\\%") // For SQLITE, we escape all % signs
                .replace("*","%"); // And then convert the * into non-escaped % signs

        /*
         * The following three lines make sure that only _ and * are valid wildcards.
         * Any other characters are enclosed inside the \Q \E markers, which force
         * all meta-characters in between them to lose their special meaning
         */
        String javaVal = val
                    .replace("_","\\E.\\Q")
                    .replace("*","\\E.*\\Q");
        /*
         * For the pattern, we use the javaVal expression that uses JAVA REGEX syntax
         */
        Pattern pattern = Pattern.compile("\\Q" + javaVal + "\\E", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

        Timber.d("start _findField model");// to measure time @TODO please remove it if this request is merged
        // find models that have that field
        Map<Long, Object[]> mods = new HashMap<>();
        try {
            for (JSONObject m : mCol.getModels().all()) {
                JSONArray flds = m.getJSONArray("flds");
                for (int fi = 0; fi < flds.length(); ++fi) {
                    JSONObject f = flds.getJSONObject(fi);
                    if (f.getString("name").equalsIgnoreCase(field)) {
                        mods.put(m.getLong("id"), new Object[] { m, f.getInt("ord") });
                    }
                }
            }
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }

        Timber.d("end _findField model");// to measure time @TODO please remove it if this request is merged
        if (mods.isEmpty()) {
            // nothing has that field
            return null;
        }
        LinkedList<Long> nids = new LinkedList<>();
        Cursor cur = null;
        Timber.d("start _findField sqlquery");// to measure time @TODO please remove it if this request is merged
        try {
            /*
             * Here we use the sqlVal expression, that is required for LIKE syntax in sqllite.
             * There is no problem with special characters, because only % and _ are special
             * characters in this syntax.
             */
            cur = mCol.getDb().getDatabase().rawQuery(
                    "select id, mid, flds from notes where mid in " +
                            Utils.ids2str(new LinkedList<>(mods.keySet())) +
                            " and flds like ? escape '\\'", new String[] { "%" + sqlVal + "%" });

            while (cur.moveToNext()) {
                String[] flds = Utils.splitFields(cur.getString(2));
                int ord = (Integer)mods.get(cur.getLong(1))[1];
                String strg = flds[ord];
                if (pattern.matcher(strg).matches()) {
                    nids.add(cur.getLong(0));
                }
            }
        } finally {
            if (cur != null) {
                cur.close();
            }
        }
        Timber.d("end _findField sqlquery");// to measure time @TODO please remove it if this request is merged
        if (nids.isEmpty()) {
            return "0";
        }
        return "n.id in " + Utils.ids2str(nids);
    }


    private String _findDupes(String val) {
        // caller must call stripHTMLMedia on passed val
        String[] split = val.split(",", 1);
        if (split.length != 2) {
            return null;
        }
        String mid = split[0];
        val = split[1];
        String csum = Long.toString(Utils.fieldChecksum(val));
        List<Long> nids = new ArrayList<>();
        Cursor cur = null;
        try {
            cur = mCol.getDb().getDatabase().rawQuery(
                    "select id, flds from notes where mid=? and csum=?",
                    new String[] { mid, csum });
            long nid = cur.getLong(0);
            String flds = cur.getString(1);
            if (Utils.stripHTMLMedia(Utils.splitFields(flds)[0]).equals(val)) {
                nids.add(nid);
            }
        } finally {
            if (cur != null) {
                cur.close();
            }
        }
        return "n.id in " +  Utils.ids2str(nids);
    }


    /**
     * Find and replace
     * ***********************************************************
     */

    /**
     * Find and replace fields in a note
     *
     * @param col The collection to search into.
     * @param nids The cards to be searched for.
     * @param src The original text to find.
     * @param dst The text to change to.
     * @param regex If true, the src is treated as a regex. Default = false.
     * @param field Limit the search to specific field. If null, it searches all fields.
     * @param fold If true the search is case-insensitive. Default = true.
     * @return
     */
    public static int findReplace(Collection col, List<Long> nids, String src, String dst) {
        return findReplace(col, nids, src, dst, false, null, true);
    }


    public static int findReplace(Collection col, List<Long> nids, String src, String dst, boolean regex) {
        return findReplace(col, nids, src, dst, regex, null, true);
    }


    public static int findReplace(Collection col, List<Long> nids, String src, String dst, String field) {
        return findReplace(col, nids, src, dst, false, field, true);
    }


    public static int findReplace(Collection col, List<Long> nids, String src, String dst, boolean isRegex,
            String field, boolean fold) {
        Map<Long, Integer> mmap = new HashMap<>();
        if (field != null) {
            try {
                for (JSONObject m : col.getModels().all()) {
                    JSONArray flds = m.getJSONArray("flds");
                    for (int fi = 0; fi < flds.length(); ++fi) {
                        JSONObject f = flds.getJSONObject(fi);
                        if (f.getString("name").equals(field)) {
                            mmap.put(m.getLong("id"), f.getInt("ord"));
                        }
                    }
                }
            } catch (JSONException e) {
                throw new RuntimeException(e);
            }
            if (mmap.isEmpty()) {
                return 0;
            }
        }
        // find and gather replacements
        if (!isRegex) {
            src = Pattern.quote(src);
        }
        if (fold) {
            src = "(?i)" + src;
        }
        Pattern regex = Pattern.compile(src);

        ArrayList<Object[]> d = new ArrayList<>();
        String snids = Utils.ids2str(nids);
        nids = new ArrayList<>();
        Cursor cur = null;
        try {
            cur = col.getDb().getDatabase().rawQuery(
                    "select id, mid, flds from notes where id in " + snids, null);
            while (cur.moveToNext()) {
                String flds = cur.getString(2);
                String origFlds = flds;
                // does it match?
                String[] sflds = Utils.splitFields(flds);
                if (field != null) {
                    long mid = cur.getLong(1);
                    if (!mmap.containsKey(mid)) {
                        // note doesn't have that field
                        continue;
                    }
                    int ord = mmap.get(mid);
                    sflds[ord] = regex.matcher(sflds[ord]).replaceAll(dst);
                } else {
                    for (int i = 0; i < sflds.length; ++i) {
                        sflds[i] = regex.matcher(sflds[i]).replaceAll(dst);
                    }
                }
                flds = Utils.joinFields(sflds);
                if (!flds.equals(origFlds)) {
                    long nid = cur.getLong(0);
                    nids.add(nid);
                    d.add(new Object[] { flds, Utils.intNow(), col.usn(), nid }); // order based on query below
                }
            }
        } finally {
            if (cur != null) {
                cur.close();
            }
        }
        if (d.isEmpty()) {
            return 0;
        }
        // replace
        col.getDb().executeMany("update notes set flds=?,mod=?,usn=? where id=?", d);
        long[] pnids = Utils.toPrimitive(nids);
        col.updateFieldCache(pnids);
        col.genCards(pnids);
        return d.size();
    }


    public List<String> fieldNames(Collection col) {
        return fieldNames(col, true);
    }

    public List<String> fieldNames(Collection col, boolean downcase) {
        Set<String> fields = new HashSet<>();
        List<String> names = new ArrayList<>();
        try {
            for (JSONObject m : col.getModels().all()) {
                JSONArray flds = m.getJSONArray("flds");
                for (int fi = 0; fi < flds.length(); ++fi) {
                    JSONObject f = flds.getJSONObject(fi);
                    if (!fields.contains(f.getString("name").toLowerCase(Locale.US))) {
                        names.add(f.getString("name"));
                        fields.add(f.getString("name").toLowerCase(Locale.US));
                    }
                }
            }
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
        if (downcase) {
            return new ArrayList<>(fields);
        }
        return names;
    }


    /**
     * Find duplicates
     * ***********************************************************
     */

    public static Integer ordForMid(Collection col, Map<Long, Integer> fields, long mid, String fieldName) {
        if (!fields.containsKey(mid)) {
            JSONObject model = col.getModels().get(mid);
            try {
                JSONArray flds = model.getJSONArray("flds");
                for (int c = 0; c < flds.length(); c++) {
                    JSONObject f = flds.getJSONObject(c);
                    if (f.getString("name").equalsIgnoreCase(fieldName)) {
                        fields.put(mid, c);
                        break;
                    }
                }
            } catch (JSONException e) {
                throw new RuntimeException(e);
            }
        }
        return fields.get(mid);
    }


    public static List<Pair<String, List<Long>>> findDupes(Collection col, String fieldName) {
        return findDupes(col, fieldName, "");
    }


    /**
     * @return List of Pair("dupestr", List[nids])
     */
    public static List<Pair<String, List<Long>>> findDupes(Collection col, String fieldName, String search) {
        // limit search to notes with applicable field name
    	if (!TextUtils.isEmpty(search)) {
            search = "(" + search + ") ";
    	}
        search += "'" + fieldName + ":*'";
        // go through notes
        Map<String, List<Long>> vals = new HashMap<>();
        List<Pair<String, List<Long>>> dupes = new ArrayList<>();
        Map<Long, Integer> fields = new HashMap<>();
        Cursor cur = null;
        try {
            cur = col.getDb().getDatabase().rawQuery(
                    "select id, mid, flds from notes where id in " + Utils.ids2str(col.findNotes(search)), null);
            while (cur.moveToNext()) {
                long nid = cur.getLong(0);
                long mid = cur.getLong(1);
                String[] flds = Utils.splitFields(cur.getString(2));
                Integer ord = ordForMid(col, fields, mid, fieldName);
                if (ord == null) {
                    continue;
                }
                String val = flds[fields.get(mid)];
                val = Utils.stripHTMLMedia(val);
                // empty does not count as duplicate
                if (TextUtils.isEmpty(val)) {
                    continue;
                }
                if (!vals.containsKey(val)) {
                    vals.put(val, new ArrayList<Long>());
                }
                vals.get(val).add(nid);
                if (vals.get(val).size() == 2) {
                    dupes.add(new Pair<>(val, vals.get(val)));
                }
            }
        } finally {
            if (cur != null) {
                cur.close();
            }
        }
        return dupes;
    }

    /*
     * ***********************************************************
     * The methods below are not in LibAnki.
     * ***********************************************************
     */

    public List<Map<String, String>> findCardsForCardBrowser(String query, boolean _order, Map<String, String> deckNames) {
        return _findCardsForCardBrowser(query, _order, deckNames);
    }


    public List<Map<String, String>> findCardsForCardBrowser(String query, String _order, Map<String, String> deckNames) {
        return _findCardsForCardBrowser(query, _order, deckNames);
    }


    /** Return a list of card ids for QUERY */
    private List<Map<String, String>> _findCardsForCardBrowser(String query, Object _order, Map<String, String> deckNames) {
        Timber.d("start findCards");// to measure time @TODO please remove it if this request is merged
        String[] tokens = _tokenize(query);
        Timber.d("_tokenize done");// to measure time @TODO please remove it if this request is merged
        Pair<String, String[]> res1 = _where(tokens);
        Timber.d("_where done");// to measure time @TODO please remove it if this request is merged
        String preds = res1.first;
        String[] args = res1.second;
        List<Map<String, String>> res = new ArrayList<>();
        if (preds == null) {
            return res;
        }
        Pair<String, Boolean> res2 = _order instanceof Boolean ? _order((Boolean) _order) : _order((String) _order);
        String order = res2.first;
        boolean rev = res2.second;
        Timber.d("start query for CardBrowser");// to measure time @TODO please remove it if this request is merged
        String sql = _queryForCardBrowser(preds, order);
        Cursor cur = null;
        try {
            cur = mCol.getDb().getDatabase().rawQuery(sql, args);
            DeckTask task = DeckTask.getInstance();
            while (cur.moveToNext()) {
                // cancel if the launching task was cancelled. 
                if (task.isCancelled()){
                    Timber.i("_findCardsForCardBrowser() cancelled...");
                    return null;
                }                
                Map<String, String> map = new HashMap<>();
                map.put("id", cur.getString(0));
                map.put("sfld", cur.getString(1));
                map.put("deck", deckNames.get(cur.getString(2)));
                int queue = cur.getInt(3);
                String tags = cur.getString(4);
                map.put("flags", Integer.toString((queue == -1 ? 1 : 0) + (tags.matches(".*[Mm]arked.*") ? 2 : 0)));
                map.put("tags", tags);
                res.add(map);
                // add placeholder for question and answer
                map.put("question", "");
                map.put("answer", "");
            }
        } catch (SQLException e) {
            // invalid grouping
            Timber.e("Invalid grouping, sql: " + sql);
            return new ArrayList<>();
        } finally {
            if (cur != null) {
                cur.close();
            }
        }
        Timber.d("end query for CardBrowser");// to measure time @TODO please remove it if this request is merged
        if (rev) {
            Collections.reverse(res);
        }
        return res;
    }
    
    /**
     * A copy of _query() with a custom SQL query specific to the AnkiDroid card browser.
     */
    private String _queryForCardBrowser(String preds, String order) {
        String sql = "select c.id, n.sfld, c.did, c.queue, n.tags from cards c, notes n where c.nid=n.id and ";
        // combine with preds
        if (!TextUtils.isEmpty(preds)) {
            sql += "(" + preds + ")";
        } else {
            sql += "1";
        }
        // order
        if (!TextUtils.isEmpty(order)) {
            sql += " " + order;
        }
        return sql;
    }
}
