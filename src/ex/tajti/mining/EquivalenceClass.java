package ex.tajti.mining;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Egy ekvivalencia osztályt reprezentál. Minden ekvivalencia osztály egy attribútum
 * halmazhoz tartozik. Azok a sorok tartalmaznak egy ekvivalencia osztályba,
 * amelyek az attribútum halmazon megegyeznek. Azaz ha adott az <code>X</code>
 * attribútum halmaz, akkor <code>t</code> és <code>u</code> pontosan akkor tartozik
 * egy ekvivalencia osztályba <code>X</code> szerint, ha <code>t[X] = u[X]</code>.
 * <br/>
 * Egy ekvivalencia osztályban nem a teljes sorokat tároljuk, hanem csak azok azonosítóját.
 * Ezen kívül bizonyos esetekben szükség lehet az attribútumon felvett közös érték
 * tárolására is. Ebben az implementációban ez csak az egyelemű attribútum halmazokra
 * lehetséges.
 * <br/>
 * Az osztály nem szálbiztos, de megfelelő örültekintéssel használható konkurrens
 * alkalmazásokban is.
 *
 * @author tajti ákos
 * @param <T> Annak az attribútumnak a típusa, amihez az osztály tartozik. Erre talán
 * nincs szükség, mivel ha több attribútumról van szó, akkor nincs értelme.
 * @param <U> A sorazonosító típusa
 */
public class EquivalenceClass<T, U> {
    /**
     * Annak az attribútum halmaznak a reprezentációja, amihez az osztály tartozik.
     * Ha a halmaz több attribútumból áll, akkor az egyes attribútumok :-tal vannak
     * elválasztva a sztringben.
     */
    private String attribute;

    /**
     * A sorok közös értéke az attribútum halmazon.
     */
    private T classifier;

    /**
     * Az ekvivalencia osztály sorainak azonosítóit tároló kllekció.
     */
    List<U> rows;

    U randomRowId = null;

    /**
     * Példányosítja az osztályt.
     */
    public EquivalenceClass(){
        this( null);
    }

    /**
     * Példányosítja az osztályt a közös érték megadásával.
     *
     * @param classifier A sorok közös értéke az attribútumon.
     */
    public EquivalenceClass(T classifier){
        this.classifier = classifier;
        rows = new ArrayList<U>();
    }

    /**
     * Eltávolít minden sort az ekvivalencia osztályból az első kivételével. A
     * verifikációhoz használom (minta hasnálatánál).
     */
//    public void removeAllRowsExceptTheFirst(){
//        List<U> l = Collections.singletonList(rows.get(0));
//        rows.retainAll(l);
//    }

    /**
     * Hozzáad egy sort (pontosabban az azonosítóját) az ekvivalencia osztályhoz.
     *
     * @param rowId A sor azonosítója.
     */
    public void addRow(U rowId){
        if(getRandomRowId() == null) //apró optimalizáció: így nem kell mindig iterátorral elkérni egy elemet a setből, de lehet, nem is éri meg
            randomRowId = rowId;
        rows.add(rowId);
    }

    /**
     * Megnézi, hogy a <code>rowId</code> azonosító benne van-e az ekvivalencia osztályban-
     *
     * @param rowId A keresett atonosító
     * @return <code>true</code>, ha az azonosító benne van az ekvivalencia osztályban,
     * <code>false</code> egyébként.
     */
    public Boolean contains(U rowId){
        return getRows().contains(rowId);
    }

    /**
     * Visszaadja a sorok közös értékét az ekvivalencia osztály attribútum halmazán.
     * 
     * @return
     */
    public T getClassifier(){
        return classifier;
    }

    /**
     * Visszaadja az attribútumot, amihez az ekvivalencia osztály tartozik.
     *
     * @return
     */
    public String getAttribute(){
        return attribute;
    }

    /**
     * Visszaadja az ekvivalencia osztály sorainak számát.
     *
     * @return
     */
    public Integer getSize(){
        return rows.size();
    }

    @Override
    public String toString(){
        return "[" + classifier + ": " + getRows() + "]";
    }

    /**
     * Visszadja az ekvivalencia osztály sorazonosítóinak listáját.
     *
     * @return the rows
     */
    public List<U> getRows() {
        return rows;
    }

    /**
     * Visszaad egy vletlenül választott sorazonosítót az ekvivalencia osztályból.
     *
     * @return 
     */
    U getRandomRowId() {
        return randomRowId;
    }

    /**
     * Sorazonosítók egy listáját hozzáadja az ekvivalencia osztályhoz.
     *
     * @param ids A sorazonosító lista.
     */
    public void addRows(List<U> ids){
        rows.addAll(ids);
    }

}
