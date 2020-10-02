/**
 * @framework       Bluemail Framework 
 * @version         1.0
 * @author          iResponse Team
 * @date            2018
 * @name            $P{MODEL}.java	
 */
package tech.bluemail.platform.models;

import java.io.Serializable;
import tech.bluemail.platform.exceptions.DatabaseException;
import tech.bluemail.platform.meta.annotations.Column;
import tech.bluemail.platform.orm.ActiveRecord;

public class $P{MODEL} extends ActiveRecord implements Serializable
{
    // columns
    $P{COLUMNS}
    // constructors
    public $P{MODEL}() throws DatabaseException 
    { 
        super(); 
        
        // database
        this.setDatabase("$P{DATABASE}");
        
        // schema
        this.setSchema("$P{SCHEMA}");
        
        // table
        this.setTable("$P{TABLE}");
    }

    public $P{MODEL}(Object primaryValue) throws DatabaseException
    {
        super(primaryValue);
        
        // database
        this.setDatabase("$P{DATABASE}");
        
        // schema
        this.setSchema("$P{SCHEMA}");
        
        // table
        this.setTable("$P{TABLE}");
        
        // load the record
        this.load();
    }
}
